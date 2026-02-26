import os
import json
import time
import csv
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd


# ----------------------------
# Config
# ----------------------------
API_KEY = os.getenv("TWELVE_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"

DATA_DIR = Path("data")
LATEST_PATH = DATA_DIR / "latest.json"
HISTORY_PATH = DATA_DIR / "history.json"

# EMA windows you use in dashboard logic
EMA_WINDOWS = [20, 50, 200]

# For daily incremental update, we only need enough candles to compute EMA200
INCR_CANDLES = 260

# Rate-limit safety: TwelveData free tiers can be strict. Keep it conservative.
SLEEP_EVERY = 8       # sleep after every N API calls
SLEEP_SECONDS = 2.0   # how long to sleep
RETRY_COUNT = 3       # retries per request
RETRY_SLEEP = 2.0     # base retry sleep


# ----------------------------
# Helpers
# ----------------------------
def ensure_data_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LATEST_PATH.exists():
        LATEST_PATH.write_text("{}", encoding="utf-8")
    if not HISTORY_PATH.exists():
        HISTORY_PATH.write_text("[]", encoding="utf-8")


def load_symbols_csv(path="nse500_symbols.csv"):
    """
    Expect format:
    symbol
    RELIANCE
    TCS
    ...
    """
    symbols = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "symbol" not in (reader.fieldnames or []):
            raise RuntimeError("nse500_symbols.csv must have a header column named: symbol")
        for row in reader:
            s = (row.get("symbol") or "").strip()
            if not s:
                continue
            # normalize common user inputs
            s = s.replace("NSE:", "").replace(".NS", "").strip().upper()
            symbols.append(s)

    # de-dupe while preserving order
    seen = set()
    unique = []
    for s in symbols:
        if s not in seen:
            unique.append(s)
            seen.add(s)

    if len(unique) < 50:
        raise RuntimeError(f"Need at least 50 symbols in nse500_symbols.csv (found {len(unique)}).")

    return unique


def td_request(params):
    """
    Robust request with retries + basic error handling.
    """
    if not API_KEY:
        raise RuntimeError("Missing TWELVE_API_KEY secret. Add it in repo Settings → Secrets and variables → Actions.")

    params = dict(params)
    params["apikey"] = API_KEY

    last_err = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            data = r.json()

            # Twelve Data sometimes returns {"status":"error","message":...}
            if isinstance(data, dict) and data.get("status") == "error":
                msg = data.get("message", "Unknown Twelve Data error")
                # Retry on rate limit / transient
                if "rate" in msg.lower() or "limit" in msg.lower() or "temporarily" in msg.lower():
                    raise RuntimeError(msg)
                # Non-retry errors
                raise RuntimeError(msg)

            return data

        except Exception as e:
            last_err = e
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_SLEEP * attempt)
                continue
            raise RuntimeError(f"TwelveData request failed after retries: {last_err}") from last_err


def fetch_series(symbol, outputsize, interval="1day"):
    """
    Fetch candles; returns DataFrame with columns: datetime, close
    """
    data = td_request({
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "format": "JSON",
        "order": "ASC",  # oldest -> newest for easier EMA
    })

    values = data.get("values", [])
    if not values:
        # Could be invalid symbol; skip later
        return pd.DataFrame(columns=["datetime", "close"])

    df = pd.DataFrame(values)
    # Ensure types
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).sort_values("datetime")
    return df[["datetime", "close"]]


def add_emas(df):
    """
    Adds EMA20/50/200 columns to df (expects close column).
    """
    out = df.copy()
    for w in EMA_WINDOWS:
        out[f"ema{w}"] = out["close"].ewm(span=w, adjust=False).mean()
    return out


def compute_health_for_date(per_symbol_df_map, dt):
    """
    Market health = % of symbols whose close > EMA200 on that date.
    If a symbol doesn't have that date candle, it's skipped.
    """
    total = 0
    above = 0

    for sym, df in per_symbol_df_map.items():
        row = df[df["datetime"] == dt]
        if row.empty:
            continue
        total += 1
        close = float(row["close"].iloc[0])
        ema200 = float(row["ema200"].iloc[0])
        if close > ema200:
            above += 1

    if total == 0:
        return None

    pct = round((above / total) * 100, 2)
    return {"date": dt.strftime("%Y-%m-%d"), "above200": above, "total": total, "health_pct": pct}


def load_history():
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_latest(obj):
    LATEST_PATH.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def save_history(arr):
    HISTORY_PATH.write_text(json.dumps(arr, indent=2), encoding="utf-8")


# ----------------------------
# Main modes
# ----------------------------
def run_incremental(symbols):
    """
    Fast path: fetch last ~260 candles per symbol, compute health only for the latest common date,
    append to history if new.
    """
    per_symbol = {}
    api_calls = 0

    for i, sym in enumerate(symbols, start=1):
        df = fetch_series(sym, outputsize=INCR_CANDLES)
        api_calls += 1
        if api_calls % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SECONDS)

        if df.empty or len(df) < 210:
            continue
        df = add_emas(df)
        per_symbol[sym] = df

    if not per_symbol:
        raise RuntimeError("No valid symbol data fetched. Check your symbols or API quota.")

    # Find latest date that exists in most series:
    # We'll use the max date among all, then compute on that date.
    latest_dt = max(df["datetime"].max() for df in per_symbol.values())
    latest = compute_health_for_date(per_symbol, latest_dt)
    if latest is None:
        raise RuntimeError("Could not compute health for latest date (no overlapping data).")

    # Update history (append only if new date)
    hist = load_history()
    last_date = hist[-1]["date"] if hist else None
    if last_date != latest["date"]:
        hist.append(latest)
        # Keep only last 5 years ~ 1300 trading days (optional)
        if len(hist) > 1400:
            hist = hist[-1400:]
        save_history(hist)

    save_latest(latest)
    return latest


def run_full_backfill(symbols, years=5):
    """
    Heavy path: fetch ~5y daily for each symbol, compute health for each date intersection.
    This can be slow and API-quota heavy. Use once.
    """
    per_symbol = {}
    api_calls = 0

    # Rough trading days for 5y: ~1250
    outputsize = 1500

    for sym in symbols:
        df = fetch_series(sym, outputsize=outputsize)
        api_calls += 1
        if api_calls % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SECONDS)

        if df.empty or len(df) < 210:
            continue
        df = add_emas(df)
        per_symbol[sym] = df

    if not per_symbol:
        raise RuntimeError("No valid symbol data fetched for backfill. Check symbols / API limits.")

    # Build a common date set: intersection of dates across symbols (strict) is often too small.
    # We'll instead use union of dates and compute on each date with available symbols.
    all_dates = sorted(set(pd.concat([df["datetime"] for df in per_symbol.values()]).unique()))
    results = []
    for dt in all_dates:
        obj = compute_health_for_date(per_symbol, dt)
        if obj:
            results.append(obj)

    # Keep approx last 5y points
    if len(results) > 1400:
        results = results[-1400:]

    save_history(results)
    if results:
        save_latest(results[-1])
    return results[-1] if results else {}


def main():
    ensure_data_files()
    symbols = load_symbols_csv()

    # If you want a full one-time backfill, set this to "1" in workflow env.
    force_full = os.getenv("FORCE_FULL_BACKFILL", "0").strip() == "1"

    if force_full:
        latest = run_full_backfill(symbols)
    else:
        latest = run_incremental(symbols)

    print("DONE:", latest)


if __name__ == "__main__":
    main()
