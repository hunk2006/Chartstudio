
async function getJSON(path){
  const res = await fetch(path, {cache: "no-store"});
  if(!res.ok) throw new Error("Failed to load " + path);
  return await res.json();
}

function zoneFromPct(p){
  if (p < 30) return {zone:"Danger Zone", badge:"DANGER", headline:"Risk is high. Stay defensive and selective."};
  if (p < 40) return {zone:"Wait & Watch", badge:"WAIT & WATCH", headline:"Mixed conditions. Let the market prove itself."};
  if (p < 60) return {zone:"Go Green", badge:"GO GREEN", headline:"Conditions improving. Deploy gradually with discipline."};
  return {zone:"Full Force", badge:"FULL FORCE", headline:"Broad participation. Trend-following has the edge."};
}

function setIndicator(pct){
  const el = document.getElementById("indicator");
  el.style.left = Math.max(0, Math.min(100, pct)) + "%";
}

function fmtPct(x){
  if (x === null || x === undefined) return "—";
  return Number(x).toFixed(1) + "%";
}

(async () => {
  const latest = await getJSON("./data/latest.json");
  const history = await getJSON("./data/history.json");

  document.getElementById("dt").textContent = latest.date_pretty ?? latest.dt ?? "—";
  document.getElementById("dailySignal").textContent = latest.signal ?? "—";
  document.getElementById("greenProb").textContent = (latest.green_prob_5d ?? "—") + "%";
  document.getElementById("greenAlert").textContent = latest.green_alert ?? "—";

  const health = latest.market_health_pct ?? latest.health_pct ?? null;
  document.getElementById("healthPct").textContent = (health ?? "—") + "%";

  document.getElementById("p20").textContent = fmtPct(latest.pct_above_20);
  document.getElementById("p50").textContent = fmtPct(latest.pct_above_50);
  document.getElementById("p200").textContent = fmtPct(latest.pct_above_200);
  document.getElementById("ad").textContent = latest.ad_ratio ?? "—";

  const z = zoneFromPct(Number(health ?? 0));
  document.getElementById("zoneBadge").textContent = z.badge;
  document.getElementById("headline").textContent = latest.headline ?? z.headline;

  setIndicator(Number(health ?? 0));

  // Chart
  const labels = history.map(r => r.dt);
  const p50 = history.map(r => r.pct_above_50);
  const prob = history.map(r => r.green_prob_5d);

  new Chart(document.getElementById("lineChart"),{
    type:"line",
    data:{labels, datasets:[
      {label:"% Stocks > 50 EMA", data:p50},
      {label:"Green Prob (next 5)", data:prob}
    ]},
    options:{
      responsive:true,
      plugins:{legend:{labels:{color:"#fff"}}},
      scales:{x:{ticks:{color:"#fff"}},y:{ticks:{color:"#fff"}}}
    }
  });
})();
