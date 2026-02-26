
import os, json, math
from datetime import datetime
import pandas as pd
import requests

API_KEY = os.getenv("TWELVE_API_KEY", "")
EXCHANGE = "XNSE"  # Twelve Data exchange code for NSE India
BASE = "https://api.twelvedata.com/time_series"

SYMBOLS_FILE = "nse500_symbols.csv"
LATEST_OUT = "data/latest.json"
HISTORY_OUT = "data/history.json"

# --- Helpers ---
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def pretty_date(dt_str: str) -> str:
    # dt_str is YYYY-MM-DD
    d = datetime.strptime(dt_str, "%Y-%m-%d")
    return d.strftime("%d %b %Y (%A)")

def fetch_batch(symbols):
    # Twelve Data supports batch by comma-separated symbols (up to ~120 per call) citeturn0search0turn0search8
    sym_param = ",".join([f"{s}:{EXCHANGE}" for s in symbols])
    params = dict(
        symbol=sym_param,
        interval="1day",
        outputsize=260,  # enough for 200 EMA
        apikey=API_KEY,
        format="JSON"
    )
    r = requests.get(BASE, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def parse_batch(batch_json):
    # Response is keyed by symbol in batch mode
    out = {}
    for key, payload in batch_json.items():
        if not isinstance(payload, dict) or "values" not in payload:
            continue
        df = pd.DataFrame(payload["values"])
        if df.empty:
            continue
        df["datetime"] = pd.to_datetime(df["datetime"])
        for c in ["open","high","low","close","volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.sort_values("datetime").reset_index(drop=True)
        out[key.split(":")[0]] = df
    return out

def load_symbols():
    df = pd.read_csv(SYMBOLS_FILE, comment="#")
    syms = [s.strip().upper() for s in df["symbol"].dropna().tolist() if str(s).strip()]
    # de-dup
    seen, uniq = set(), []
    for s in syms:
        if s not in seen:
            seen.add(s); uniq.append(s)
    return uniq

def compute_daily_metrics(symbol_dfs):
    # Align to latest common date across symbols (avoid partial laggers)
    last_dates = []
    enriched = []
    for sym, df in symbol_dfs.items():
        df = df.dropna(subset=["close"]).copy()
        if len(df) < 210:
            continue
        df["ema20"] = ema(df["close"], 20)
        df["ema50"] = ema(df["close"], 50)
        df["ema200"] = ema(df["close"], 200)
        df["ret1"] = df["close"].pct_change()
        last = df.dropna().iloc[-1]
        last_dates.append(last["datetime"].date())
        enriched.append((sym, last))

    if not enriched:
        raise RuntimeError("No usable symbol data. Check symbols list and API key.")

    common = min(last_dates).isoformat()
    rows = [(sym, last) for sym, last in enriched if last["datetime"].date().isoformat() == common]
    n = len(rows)

    above20 = sum(1 for _, r in rows if r["close"] > r["ema20"])
    above50 = sum(1 for _, r in rows if r["close"] > r["ema50"])
    above200 = sum(1 for _, r in rows if r["close"] > r["ema200"])

    adv = sum(1 for _, r in rows if r["ret1"] > 0)
    dec = sum(1 for _, r in rows if r["ret1"] < 0)
    ad_ratio = (adv / dec) if dec > 0 else float(adv)

    pct20 = round(100 * above20 / n, 1)
    pct50 = round(100 * above50 / n, 1)
    pct200 = round(100 * above200 / n, 1)

    # --- Your Excel-like score (starter; you can tweak to match exactly) ---
    score = 0
    score += 1 if pct20 >= 50 else 0
    score += 1 if pct50 >= 50 else 0
    score += 1 if pct200 >= 45 else 0
    score += 1 if ad_ratio >= 1.0 else 0

    if score >= 4: signal = "GREEN"
    elif score == 3: signal = "WATCH"
    elif score == 2: signal = "CAUTION"
    else: signal = "RED"

    # Market health % (0–100) derived from score + pct50 + pct200
    health_pct = max(0, min(100, round((pct50*0.6 + pct200*0.4), 1)))

    # --- “Green coming” probability model (simple + interpretable) ---
    # Base depends on current signal
    base = {"RED": 28.0, "CAUTION": 36.0, "WATCH": 45.0, "GREEN": 62.0, "STRONG GREEN": 72.0}.get(signal, 35.0)

    # Triggers (0/1)
    p50_thrust = 1 if (pct50 >= 45.0) else 0
    ad_strong = 1 if (ad_ratio >= 1.10) else 0
    p200_improving = 1 if (pct200 >= 40.0) else 0

    bonus = 0.0
    bonus += 12.0 if p50_thrust else 0.0
    bonus += 10.0 if ad_strong else 0.0
    bonus += 6.0 if p200_improving else 0.0

    prob = max(5.0, min(90.0, round(base + bonus, 1)))
    if prob >= 60: alert = "HIGH"
    elif prob >= 45: alert = "MED"
    else: alert = "LOW"

    # Headline aligned with your zone meanings
    if health_pct < 30:
        headline = "Danger zone: protect capital, reduce risk."
        badge = "DANGER"
    elif health_pct < 40:
        headline = "Wait & watch: early improvement possible, but no confirmation yet."
        badge = "WAIT & WATCH"
    elif health_pct < 60:
        headline = "Go green: participation improving — deploy gradually."
        badge = "GO GREEN"
    else:
        headline = "Full force: broad participation — trend-following has the edge."
        badge = "FULL FORCE"

    latest = dict(
        dt=common,
        date_pretty=pretty_date(common),
        signal=signal,
        score=int(score),
        pct_above_20=pct20,
        pct_above_50=pct50,
        pct_above_200=pct200,
        adv=int(adv),
        dec=int(dec),
        ad_ratio=round(float(ad_ratio), 3),
        market_health_pct=health_pct,
        green_prob_5d=prob,
        green_alert=alert,
        headline=headline,
        badge=badge,
        flags=dict(p50_thrust=p50_thrust, ad_strong=ad_strong, p200_improving=p200_improving),
        universe_count=n
    )
    return latest

def load_history():
    if os.path.exists(HISTORY_OUT):
        try:
            return json.load(open(HISTORY_OUT, "r", encoding="utf-8"))
        except Exception:
            return []
    return []

def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    if not API_KEY:
        raise RuntimeError("Missing TWELVE_API_KEY env var. Add it as a GitHub Secret.")

    symbols = load_symbols()
    if len(symbols) < 200:
        raise RuntimeError(f"Need NSE500 list in nse500_symbols.csv (found {len(symbols)} symbols).")

    all_dfs = {}
    # 120 symbols per call is supported in batch mode citeturn0search0turn0search8
    for group in chunked(symbols, 120):
        payload = fetch_batch(group)
        all_dfs.update(parse_batch(payload))

    latest = compute_daily_metrics(all_dfs)

    history = load_history()
    # upsert by dt
    history = [h for h in history if h.get("dt") != latest["dt"]]
    history.append({k: latest[k] for k in ["dt","pct_above_50","pct_above_200","green_prob_5d","market_health_pct"]})
    history = sorted(history, key=lambda x: x["dt"])[-800:]

    save_json(LATEST_OUT, latest)
    save_json(HISTORY_OUT, history)

    print("Updated:", latest["dt"], "Universe:", latest["universe_count"])

if __name__ == "__main__":
    main()
