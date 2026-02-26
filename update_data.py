import io
import os
import json
import gzip
import time
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
# Example:
# https://archives.nseindia.com/content/historical/EQUITIES/2026/FEB/cm26022026bhav.csv.zip
# -----------------------------
BHAV_BASE = "https://archives.nseindia.com/content/historical/EQUITIES"


def ensure_storage():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LATEST_PATH.exists():
        LATEST_PATH.write_text("{}", encoding="utf-8")
    if not HISTORY_PATH.exists():
        HISTORY_PATH.write_text("[]", encoding="utf-8")
    if not CLOSES_PATH.exists():
        # create empty gz csv with header
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
    df["date"] = pd.to_datetime(df["date"])
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "close"])
    return df


def write_closes(df):
    out = df.copy()
    if not out.empty:
        out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
        out["symbol"] = out["symbol"].astype(str).str.upper()
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
    # de-dupe preserve order
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
    ddmmyyyy = dt.strftime("%d%m%Y")
    return f"{BHAV_BASE}/{yyyy}/{mmm}/cm{ddmmyyyy}bhav.csv.zip"


def download_bhavcopy(dt: datetime) -> pd.DataFrame:
    """
    Downloads & parses bhavcopy zip for a date.
    Returns DataFrame with columns: SYMBOL, SERIES, CLOSE, TIMESTAMP
    """
    url = bhavcopy_url(dt)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "keep-alive",
    }
    r = requests.get(url, headers=headers, timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"Bhavcopy not available: {url} (status {r.status_code})")

    z = zipfile.ZipFile(io.BytesIO(r.content))
    # usually one csv inside
    name = z.namelist()[0]
    raw = z.read(name)
    df = pd.read_csv(io.BytesIO(raw))

    # Normalize expected columns
    df.columns = [c.strip().upper() for c in df.columns]
    for c in ["SYMBOL", "SERIES", "CLOSE", "TIMESTAMP"]:
        if c not in df.columns:
            raise RuntimeError(f"Bhavcopy missing column {c}. Columns={df.columns.tolist()}")

    # Parse date
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
    df = df.dropna(subset=["TIMESTAMP"])
    return df


def get_latest_available_bhavcopy(max_lookback_days=10):
    """
    Try today, then go back up to N days to find last trading day file.
    """
    today = datetime.now()
    for i in range(max_lookback_days):
        dt = today - timedelta(days=i)
        try:
            df = download_bhavcopy(dt)
            return dt, df
        except Exception:
            continue
    raise RuntimeError("Could not find any bhavcopy in last lookback window.")


def compute_ema_flags(closes_df: pd.DataFrame):
    """
    closes_df: columns date,symbol,close
    returns per-symbol latest close, ema200, and flag(close>ema200)
    """
    df = closes_df.sort_values(["symbol", "date"]).copy()

    # Compute EMA200 per symbol
    df["ema200"] = (
        df.groupby("symbol")["close"]
        .transform(lambda s: s.ewm(span=200, adjust=False).mean())
    )

    # Take latest row per symbol
    latest = df.groupby("symbol").tail(1).copy()
    latest["above200"] = latest["close"] > latest["ema200"]
    return latest[["date", "symbol", "close", "ema200", "above200"]]


def main():
    ensure_storage()
    symbols = load_symbols_csv("nse500_symbols.csv")

    # 1) Download latest available bhavcopy
    dt, bhav = get_latest_available_bhavcopy()
    bhav_date = pd.to_datetime(dt.strftime("%Y-%m-%d"))

    # 2) Filter EQ series only and our symbol universe
    eq = bhav[bhav["SERIES"].astype(str).str.upper() == "EQ"].copy()
    eq["SYMBOL"] = eq["SYMBOL"].astype(str).str.upper().str.strip()
    eq = eq[eq["SYMBOL"].isin(symbols)].copy()
    eq["CLOSE"] = pd.to_numeric(eq["CLOSE"], errors="coerce")
    eq = eq.dropna(subset=["CLOSE"])

    if eq.empty:
        raise RuntimeError("Bhavcopy loaded, but no matching EQ symbols found (check symbol list).")

    # 3) Load stored closes and append latest date if not already present
    closes = read_closes()

    # If this date already exists, do nothing (avoid duplicates)
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

        # Keep only last ~5 years trading days (~1400) per symbol
        closes = closes.sort_values(["symbol", "date"])
        closes = closes.groupby("symbol").tail(1400).reset_index(drop=True)

        write_closes(closes)

    # 4) Compute Market Health for latest date
    # Need enough history for EMA200; skip symbols with <210 points
    counts = closes.groupby("symbol").size()
    eligible = counts[counts >= 210].index.tolist()
    closes_eligible = closes[closes["symbol"].isin(eligible)].copy()

    latest_flags = compute_ema_flags(closes_eligible)

    # Use latest date actually present in computed flags
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

    # 5) Update history (append only if new date)
    hist = read_history()
    last_date = hist[-1]["date"] if hist else None
    if last_date != latest_obj["date"]:
        hist.append(latest_obj)
        # Keep last ~5y
        if len(hist) > 1400:
            hist = hist[-1400:]
        write_history(hist)

    write_latest(latest_obj)

    print("DONE", latest_obj)


if __name__ == "__main__":
    main()
