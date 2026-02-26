# ChartStudio • NSE 500 Market Health (Auto-updating)

This is a static HTML dashboard (GitHub Pages / Netlify) that auto-updates daily using a scheduled GitHub Action.

## Why this works on free data
We use Twelve Data's **batch request** capability (up to ~120 symbols per request), so NSE 500 can be pulled in ~5 API calls/day. See Twelve Data batch docs. citeturn0search0turn0search8

## Setup (10 minutes)
1. Create a free Twelve Data API key.
2. Put NSE 500 symbols into `nse500_symbols.csv` (one per row).
3. Push this project to your GitHub repo.
4. In GitHub repo → Settings → Secrets and variables → Actions → New repository secret:
   - Name: `TWELVE_API_KEY`
   - Value: your key
5. Enable GitHub Pages:
   - Settings → Pages → Deploy from branch → `main` → `/ (root)`
6. Open the live link from Pages (you’ll get a public URL you can open anywhere).

## Update schedule
The workflow runs Mon–Fri at the cron time inside `.github/workflows/update.yml`.
Change it if you want a different time.

## Run locally (optional)
- `python update_data.py` (with env var TWELVE_API_KEY)
- Then open `index.html` in your browser.

## Notes
- This starter computes a simple signal + green-soon probability. You can tweak thresholds inside `update_data.py` to match your Excel 1:1.
