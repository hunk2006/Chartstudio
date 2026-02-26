import io
import json
import gzip
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests


# -----------------------------
# Files
# -----------------------------
DATA_DIR = Path("data")
LATEST_PATH = DATA_DIR / "latest.json"
HISTORY_PATH = DATA_DIR / "history.json"
CLOSES_PATH = DATA_DIR / "closes.csv.gz"   # date,symbol,close


# -----------------------------
# NSE Bhavcopy URLs (Equities)
# Correct filename format:
# cmDDMMMYYYYbhav.csv.zip  (example: cm26FEB2026bhav.csv.zip)
# Example:
# https://archives.nseindia.com/content/historical/EQUITIES/2026/FEB/cm26FEB2026bhav.csv.zip
# -----------------------------
BHAV_BASE = "https://archives.nseindia.com/content/historical/EQUITIES"


def ensure_storage():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LATEST_PATH.exists():
        LATEST_PATH.write_text("{}", encoding="utf-8")
    if not HISTORY_PATH.exists():
        HISTORY_PATH.write_text("[]", encoding="utf-8")
    if not CLOSES_PATH.exists():
        df0 = pd.DataFrame(columns=["date", "symbol", "close"])
        write_closes(df0)


def read_history():
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def write_history(arr):
    HISTORY_PATH.write_text(json.dumps(arr, indent=2), encoding="utf-8")


def write_latest(obj):
    LATEST_PATH.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def read_closes():
    if not CLOSES_PATH.exists():
        return pd.DataFrame(columns=["date", "symbol", "close"])
    with gzip.open(CLOSES_PATH, "rt", encoding="utf-8") as f:
        df = pd.read_csv(f)
    if df.empty:
        return pd.DataFrame(columns=["date", "symbol", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "close"])
    return df


def write_closes(df):
    out = df.copy()
    if not out.empty:
        out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
        out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    with gzip.open(CLOSES_PATH, "wt", encoding="utf-8") as f:
        out.to_csv(f, index=False)


def load_symbols_csv(path="nse500_symbols.csv"):
    df = pd.read_csv(path)
    if "symbol" not in df.columns:
        raise RuntimeError("nse500_symbols.csv must have a header column named: symbol")
    syms = (
        df["symbol"]
        .astype(str)
        .str.strip()
        .str.replace("NSE:", "", regex=False)
        .str.replace(".NS", "", regex=False)
        .str.upper()
        .tolist()
    )
    seen = set()
    out = []
    for s in syms:
        if not s or s == "NAN":
            continue
        if s not in seen:
            out.append(s)
            seen.add(s)
    if len(out) < 100:
        raise RuntimeError(f"Too few symbols in nse500_symbols.csv (found {len(out)}).")
    return out


def bhavcopy_url(dt: datetime) -> str:
    yyyy = dt.strftime("%Y")
    mmm = dt.strftime("%b").upper()  # FEB, MAR...
    ddmmmyyyy = dt.strftime("%d%b%Y").upper()  # 26FEB2026
    return f"{BHAV_BASE}/{yyyy}/{mmm}/cm{ddmmmyyyy}bhav.csv.zip"


def download_bhavcopy(dt: datetime) -> pd.DataFrame:
    url = bhavcopy_url(dt)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/",
    }
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Bhavcopy not available: {url} (status {r.status_code})")

    z = zipfile.ZipFile(io.BytesIO(r.content))
    name = z.namelist()[0]
    raw = z.read(name)
    df = pd.read_csv(io.BytesIO(raw))

    df.columns = [c.strip().upper() for c in df.columns]
    for c in ["SYMBOL", "SERIES", "CLOSE", "TIMESTAMP"]:
        if c not in df.columns:
            raise RuntimeError(f"Bhavcopy missing column {c}. Columns={df.columns.tolist()}")

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
    df = df.dropna(subset=["TIMESTAMP"])
    return df


def get_latest_available_bhavcopy(max_lookback_days=20):
    today = datetime.now()
    last_err = None
    for i in range(max_lookback_days):
        dt = today - timedelta(days=i)
        try:
            df = download_bhavcopy(dt)
            return dt, df
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not find any bhavcopy in last lookback window. Last error: {last_err}")


def compute_ema_flags(closes_df: pd.DataFrame):
    df = closes_df.sort_values(["symbol", "date"]).copy()
    df["ema200"] = df.groupby("symbol")["close"].transform(
        lambda s: s.ewm(span=200, adjust=False).mean()
    )
    latest = df.groupby("symbol").tail(1).copy()
    latest["above200"] = latest["close"] > latest["ema200"]
    return latest[["date", "symbol", "close", "ema200", "above200"]]


def main():
    ensure_storage()
    symbols = load_symbols_csv("nse500_symbols.csv")

    # 1) Download latest available bhavcopy (lookback covers weekends/holidays)
    dt, bhav = get_latest_available_bhavcopy(max_lookback_days=20)
    bhav_date = pd.to_datetime(dt.strftime("%Y-%m-%d"))

    # 2) Filter EQ series + our symbol list
    bhav["SERIES"] = bhav["SERIES"].astype(str).str.upper()
    bhav["SYMBOL"] = bhav["SYMBOL"].astype(str).str.upper().str.strip()

    eq = bhav[bhav["SERIES"] == "EQ"].copy()
    eq = eq[eq["SYMBOL"].isin(symbols)].copy()
    eq["CLOSE"] = pd.to_numeric(eq["CLOSE"], errors="coerce")
    eq = eq.dropna(subset=["CLOSE"])

    if eq.empty:
        raise RuntimeError("Bhavcopy loaded, but no matching EQ symbols found (check symbol list).")

    # 3) Load stored closes and append latest date if missing
    closes = read_closes()

    already = False
    if not closes.empty:
        already = (closes["date"].dt.strftime("%Y-%m-%d") == bhav_date.strftime("%Y-%m-%d")).any()

    if not already:
        new_rows = pd.DataFrame({
            "date": [bhav_date] * len(eq),
            "symbol": eq["SYMBOL"].tolist(),
            "close": eq["CLOSE"].tolist(),
        })
        closes = pd.concat([closes, new_rows], ignore_index=True)

        # Keep only ~5y trading days per symbol
        closes = closes.sort_values(["symbol", "date"])
        closes = closes.groupby("symbol").tail(1400).reset_index(drop=True)
        write_closes(closes)

    # 4) Compute Market Health (close > EMA200)
    counts = closes.groupby("symbol").size()
    eligible = counts[counts >= 210].index.tolist()
    closes_eligible = closes[closes["symbol"].isin(eligible)].copy()

    if closes_eligible.empty:
        raise RuntimeError("Not enough history yet to compute EMA200. Need ~210 days of closes. Keep daily runs on.")

    latest_flags = compute_ema_flags(closes_eligible)
    latest_date = latest_flags["date"].max()
    today_slice = latest_flags[latest_flags["date"] == latest_date]

    total = int(today_slice.shape[0])
    above = int(today_slice["above200"].sum())
    health_pct = round((above / total) * 100, 2) if total else 0.0

    latest_obj = {
        "date": pd.to_datetime(latest_date).strftime("%Y-%m-%d"),
        "above200": above,
        "total": total,
        "health_pct": health_pct,
    }

    # 5) Append to history if new date
    hist = read_history()
    last_date = hist[-1]["date"] if hist else None
    if last_date != latest_obj["date"]:
        hist.append(latest_obj)
        if len(hist) > 1400:
            hist = hist[-1400:]
        write_history(hist)

    write_latest(latest_obj)
    print("DONE", latest_obj)


if __name__ == "__main__":
    main()
