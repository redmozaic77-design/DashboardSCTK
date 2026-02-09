// ==========================================================
// FULL STATIC DASHBOARD:
// - Kuantitas: MQTT over WebSocket (mqtt.js)
// - QC: Google Sheets publish CSV
// - NO backend (/api/*, /events) lagi
// ==========================================================

// ===================== KONFIG =====================
// MQTT
const TOPIC = "data/sctkiotserver/groupsctkiotserver/123";

// coba beberapa kandidat (edit kalau kamu sudah tahu yang bener)
const WS_CANDIDATES = [
  "ws://103.217.145.168:8083/mqtt",
  "ws://103.217.145.168:8083",
  "ws://103.217.145.168:9001",
  "wss://103.217.145.168:8084/mqtt",
];

const NUMERIC_KEYS = [
  "PRESSURE_DST",
  "LVL_RES_WTP3",
  "TOTAL_FLOW_ITK",
  "TOTAL_FLOW_DST",
  "FLOW_WTP3",
  "FLOW_50_WTP1",
  "FLOW_CIJERUK",
  "FLOW_CARENANG",
];

// QC CSV (publish CSV)
const QC_CSV_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vSMKrU7GU9pisN4ihKgSqyC1bDuT1ia6kp-vKWrdUhvaPyX95ZqOBOFy8iBCpQieizqTBJ3R4wNmRII/pub?gid=2046456175&single=true&output=csv";
const QC_PULL_INTERVAL_MS = 20000;

// ======= konstanta dari backend (kalau tidak ada, fallback) =======
const RES_MAX_M = Number(window.RES_MAX_M || 8.0);
const RES_LITER_PER_M = Number(window.RES_LITER_PER_M || 375000.0);

// ===================== UTIL =====================
function cssVar(name){ return getComputedStyle(document.body).getPropertyValue(name).trim(); }
function clamp(x,a,b){ return Math.max(a, Math.min(b, x)); }
function fmt(n, d=2){
  const x = Number(n);
  if (!isFinite(x)) return "-";
  return x.toFixed(d);
}
function fmtTime(tsSec, withSec=false){
  const d = new Date(tsSec*1000);
  return d.toLocaleTimeString([], withSec
    ? {hour:"2-digit", minute:"2-digit", second:"2-digit"}
    : {hour:"2-digit", minute:"2-digit"}
  );
}
async function fetchTextNoCache(url){
  const sep = url.includes("?") ? "&" : "?";
  const u = url + sep + "_=" + Date.now();
  const res = await fetch(u, { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return await res.text();
}

// ===================== THEME =====================
function applyTheme(theme){
  document.body.dataset.theme = theme;
  localStorage.setItem("theme", theme);
  const btn = document.getElementById("themeToggle");
  if (btn) btn.textContent = (theme === "dark") ? "LIGHT" : "DARK";
  redrawAllCharts();
}
function initTheme(){
  const saved = localStorage.getItem("theme");
  if (saved === "light" || saved === "dark") applyTheme(saved);
  else applyTheme("dark");
  const btn = document.getElementById("themeToggle");
  if (btn){
    btn.addEventListener("click", () => {
      applyTheme(document.body.dataset.theme === "dark" ? "light" : "dark");
    });
  }
}
function setChartDefaults(){
  if (!window.Chart) return;
  Chart.defaults.color = cssVar("--muted");
  Chart.defaults.borderColor = cssVar("--stroke");
  Chart.defaults.font.family = "Inter, Arial";
}

// ===================== SLIDER =====================
const SLIDE_INTERVAL_MS = 10000;
let slideIndex = 0;
let slideTimer = null;
let isSlidePaused = false;

function setSlide(idx){
  slideIndex = (idx % 2 + 2) % 2;
  const slides = document.getElementById("slides");
  if (!slides) return;
  slides.style.transform = `translateX(-${slideIndex * 50}%)`;
}
function nextSlide(){ setSlide(slideIndex + 1); }
function prevSlide(){ setSlide(slideIndex - 1); }

function startAutoSlide(){
  stopAutoSlide();
  if (isSlidePaused) return;
  slideTimer = setInterval(() => nextSlide(), SLIDE_INTERVAL_MS);
}
function stopAutoSlide(){
  if (slideTimer){ clearInterval(slideTimer); slideTimer = null; }
}

function setPauseUI(paused){
  const btn = document.getElementById("slideToggle");
  const ico = document.getElementById("slideToggleIcon");
  if (!btn || !ico) return;

  if (paused){
    btn.classList.add("paused");
    btn.title = "Play slide";
    btn.setAttribute("aria-label","Play slide");
    ico.innerHTML = '<path fill="currentColor" d="M8 5v14l11-7z"/>';
  } else {
    btn.classList.remove("paused");
    btn.title = "Pause slide";
    btn.setAttribute("aria-label","Pause slide");
    ico.innerHTML = '<path fill="currentColor" d="M7 5h3v14H7zM14 5h3v14h-3z"/>';
  }
}
function setSlidePaused(paused){
  isSlidePaused = !!paused;
  localStorage.setItem("slidePaused", isSlidePaused ? "1" : "0");
  setPauseUI(isSlidePaused);
  if (isSlidePaused) stopAutoSlide();
  else startAutoSlide();
}
function initSlidePause(){
  isSlidePaused = (localStorage.getItem("slidePaused") === "1");
  setPauseUI(isSlidePaused);

  const btn = document.getElementById("slideToggle");
  if (btn){
    btn.addEventListener("click", () => setSlidePaused(!isSlidePaused));
  }
}

function setupNavButtons(){
  const btnNext = document.getElementById("btnNext");
  const btnPrev = document.getElementById("btnPrev");

  if (btnNext) btnNext.addEventListener("click", () => { nextSlide(); if (!isSlidePaused) startAutoSlide(); });
  if (btnPrev) btnPrev.addEventListener("click", () => { prevSlide(); if (!isSlidePaused) startAutoSlide(); });

  const nearDist = 120;
  function setNear(btn, isNear){
    if (!btn) return;
    if (isNear) btn.classList.add("near");
    else btn.classList.remove("near");
  }

  document.addEventListener("mousemove", (e) => {
    for (const btn of [btnPrev, btnNext]){
      if (!btn) continue;
      const r = btn.getBoundingClientRect();
      const cx = r.left + r.width/2;
      const cy = r.top + r.height/2;
      const dx = e.clientX - cx;
      const dy = e.clientY - cy;
      const dist = Math.sqrt(dx*dx + dy*dy);
      setNear(btn, dist < nearDist);
    }
  }, { passive: true });
}

// ===================== RESERVOIR =====================
function buildReservoirLabels(){
  const labels = document.getElementById("lvl_labels");
  const major = document.getElementById("tank_major_lines");
  if (!labels || !major) return;
  labels.innerHTML = "";
  major.innerHTML = "";
  const max = RES_MAX_M;
  for (let m=0; m<=max; m++){
    const pct = (m / max) * 100;
    const el = document.createElement("div");
    el.className = "yl";
    el.style.bottom = pct + "%";
    el.textContent = m + "M";
    labels.appendChild(el);

    const line = document.createElement("div");
    line.className = "ml";
    line.style.bottom = pct + "%";
    major.appendChild(line);
  }
}

function animateReservoirFillTo(pct){
  const water = document.getElementById("water_level");
  const marker = document.getElementById("lvl_marker");
  if (!water || !marker) return;

  water.style.transition = "none";
  marker.style.transition = "none";
  water.style.height = "0%";
  marker.style.bottom = "0%";

  requestAnimationFrame(() => {
    water.style.transition = "height .70s ease";
    marker.style.transition = "bottom .70s ease";
    water.style.height = pct + "%";
    marker.style.bottom = pct + "%";
  });
}

function updateReservoir(levelM, animate=true){
  const v = clamp(Number(levelM)||0, 0, RES_MAX_M);
  const pct = Math.round((v / RES_MAX_M) * 100);

  const pctEl = document.getElementById("pct_LVL");
  if (pctEl) pctEl.textContent = String(pct);

  if (animate) animateReservoirFillTo(pct);
  else {
    const water = document.getElementById("water_level");
    const marker = document.getElementById("lvl_marker");
    if (water && marker){
      water.style.height = pct + "%";
      marker.style.bottom = pct + "%";
    }
  }
}

// ===================== GAUGE =====================
let gaugeChart = null;
function ensureGauge(){
  const canvas = document.getElementById("gauge_pressure");
  if (!canvas || !window.Chart) return null;
  const ctx = canvas.getContext("2d");
  if (gaugeChart) return gaugeChart;

  gaugeChart = new Chart(ctx, {
    type: "doughnut",
    data: { datasets: [{
      data: [0, 5],
      backgroundColor: [cssVar("--accent"), cssVar("--darkArc")],
      borderWidth: 0,
      cutout: "72%"
    }]},
    options: {
      rotation: -90,
      circumference: 180,
      responsive: true,
      maintainAspectRatio: true,
      aspectRatio: 2,
      animation: { duration: 650, easing: "easeOutQuart" },
      plugins: { legend: { display:false }, tooltip: { enabled:false } }
    }
  });
  return gaugeChart;
}

function updatePressureGauge(val, animate=true){
  const max = 5;
  const v = clamp(Number(val)||0, 0, max);
  const ch = ensureGauge();
  if (!ch) return;

  ch.data.datasets[0].backgroundColor = [cssVar("--accent"), cssVar("--darkArc")];

  if (!animate){
    ch.options.animation = false;
    ch.data.datasets[0].data = [v, max - v];
    ch.update();
    return;
  }

  ch.options.animation = false;
  ch.data.datasets[0].data = [0, max];
  ch.update();

  requestAnimationFrame(() => {
    ch.options.animation = { duration: 650, easing: "easeOutQuart" };
    ch.data.datasets[0].data = [v, max - v];
    ch.update();
  });
}

// ===================== CHART HELPERS =====================
const POP_ANIM = { duration: 650, easing: "easeOutQuart" };
function yFromBaseline(ctx){
  const y = ctx.chart.scales?.y;
  if(!y) return 0;
  const min = y.min;
  const max = y.max;
  let base = 0;
  if (base < min || base > max) base = min;
  return base;
}

// ==========================================================
// KUANTITAS (spark + big chart) -> source: MQTT
// ==========================================================
const QTY_TILE_SHIFT_SEC = 10;
const QTY_TILE_POINTS    = 18;

const qtyTileCharts = {};
const qtyTileSeries = {};
const qtyTileKeys = Array.from(document.querySelectorAll("canvas.spark")).map(c => c.dataset.key);

function ensureQtySeries(key){
  if (!qtyTileSeries[key]) qtyTileSeries[key] = { labels: [], data: [], lastBucket: null };
  return qtyTileSeries[key];
}

function createQtyTileChart(ctx, labels, data){
  return new Chart(ctx, {
    type: "line",
    data: {
      labels: labels || [],
      datasets: [{
        data: data || [],
        borderColor: cssVar("--accent"),
        backgroundColor: cssVar("--accentFill"),
        fill: true,
        borderWidth: 2,
        tension: 0.30,
        pointRadius: 2,
        pointHoverRadius: 0,
        pointHitRadius: 0,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: POP_ANIM,
      animations: { y: { from: (ctx) => yFromBaseline(ctx) } },
      plugins: { legend: { display:false }, tooltip: { enabled:false } },
      scales: {
        x: { grid: { color: cssVar("--grid"), display: true } },
        y: { grid: { color: cssVar("--grid"), display: true } }
      }
    }
  });
}

function renderQtyTile(key){
  const canvas = document.getElementById("spark_" + key);
  if (!canvas) return;
  const s = ensureQtySeries(key);

  if (qtyTileCharts[key]) qtyTileCharts[key].destroy();
  qtyTileCharts[key] = createQtyTileChart(canvas.getContext("2d"), [...s.labels], [...s.data]);
}

function initQtyTileCharts(){
  for (const key of qtyTileKeys){
    ensureQtySeries(key);
    renderQtyTile(key);
  }
}

// Big chart kuantitas (pakai data rolling dari MQTT)
let qtyChart = null;
let qtyBigSeries = { labels: [], data: [], lastBucket: null };
function qtyLabel(key){
  const map = {
    "TOTAL_FLOW_DST":"TOTAL FLOW DISTRIBUSI",
    "TOTAL_FLOW_ITK":"TOTAL FLOW INTAKE",
    "SELISIH_FLOW":"SELISIH FLOW",
    "FLOW_WTP3":"FLOW WTP 3",
    "FLOW_50_WTP1":"FLOW UPAM CIKANDE",
    "FLOW_CIJERUK":"FLOW UPAM CIJERUK",
    "FLOW_CARENANG":"FLOW UPAM CARENANG",
    "PRESSURE_DST":"PRESSURE DISTRIBUSI",
    "LVL_RES_WTP3":"LEVEL RESERVOIR WTP 3"
  };
  return map[key] || key;
}

function ensureQtyBigChart(){
  const canvas = document.getElementById("chartBig");
  if (!canvas || !window.Chart) return;
  const ctx = canvas.getContext("2d");
  if (qtyChart) return;

  qtyChart = new Chart(ctx, {
    type: "line",
    data: { labels: [], datasets: [{
      label: "DATA",
      data: [],
      borderColor: cssVar("--accent"),
      backgroundColor: cssVar("--accentFill"),
      fill: true,
      pointRadius: 0,
      borderWidth: 2,
      tension: 0.30
    }]},
    options: {
      responsive:true,
      maintainAspectRatio:false,
      animation: POP_ANIM,
      animations: { y: { from: (ctx) => yFromBaseline(ctx) } },
      scales: {
        x: { grid: { color: cssVar("--grid"), display:true } },
        y: { grid: { color: cssVar("--grid"), display:true } }
      }
    }
  });
}

function pushQtyBigPoint(tsSec, key, value){
  ensureQtyBigChart();
  if (!qtyChart) return;

  // bucket per 10 detik supaya smooth tapi gak kebanyakan
  const bucket = Math.floor(tsSec / 10) * 10;
  const label = fmtTime(bucket, true);

  const isNewBucket = (qtyBigSeries.lastBucket !== bucket);
  if (isNewBucket){
    qtyBigSeries.labels.push(label);
    qtyBigSeries.data.push(value);
    qtyBigSeries.lastBucket = bucket;
    // keep max ~ 300 points
    while (qtyBigSeries.labels.length > 300){
      qtyBigSeries.labels.shift();
      qtyBigSeries.data.shift();
    }
  } else {
    if (qtyBigSeries.data.length) qtyBigSeries.data[qtyBigSeries.data.length - 1] = value;
  }

  const selKey = document.getElementById("qtyParam")?.value || key;
  qtyChart.data.labels = [...qtyBigSeries.labels];
  qtyChart.data.datasets[0].data = [...qtyBigSeries.data];
  qtyChart.data.datasets[0].label = `${qtyLabel(selKey)} (MQTT)`;
  qtyChart.update();
}

// =========================
// ESTIMASI CADANGAN
// =========================
function secondsToHuman(sec){
  if (sec == null || !isFinite(sec)) return "-";
  if (sec < 0) return "-";
  if (sec < 60) return Math.round(sec) + " dtk";
  const m = Math.floor(sec/60);
  const h = Math.floor(m/60);
  const mm = m % 60;
  if (h <= 0) return mm + " menit";
  return h + " jam " + mm + " menit";
}
function computeEtaSeconds(levelM, netLps, targetM){
  const lvl = Number(levelM);
  const net = Number(netLps);
  if (!isFinite(lvl) || !isFinite(net)) return null;
  if (Math.abs(net) < 0.2) return null;

  const rate_m_per_s = net / RES_LITER_PER_M;
  const delta = targetM - lvl;

  if ((delta > 0 && rate_m_per_s <= 0) || (delta < 0 && rate_m_per_s >= 0)) return null;
  return delta / rate_m_per_s;
}
function setTrendUI(net){
  const icon = document.getElementById("trendIcon");
  const text = document.getElementById("trendText");
  const sub = document.getElementById("netStateSub");

  const upSvg = '<svg viewBox="0 0 24 24"><path d="M12 4l7 7h-4v9H9v-9H5z"/></svg>';
  const dnSvg = '<svg viewBox="0 0 24 24"><path d="M12 20l-7-7h4V4h6v9h4z"/></svg>';
  const flSvg = '<svg viewBox="0 0 24 24"><path d="M4 12h16v2H4z"/></svg>';

  icon?.classList.remove("trUp","trDown","trFlat");

  if (net > 0.2){
    icon?.classList.add("trUp");
    if (icon) icon.innerHTML = upSvg;
    if (text) text.textContent = "CADANGAN NAIK";
    if (sub) sub.textContent = "NAIK";
  } else if (net < -0.2){
    icon?.classList.add("trDown");
    if (icon) icon.innerHTML = dnSvg;
    if (text) text.textContent = "CADANGAN TURUN";
    if (sub) sub.textContent = "TURUN";
  } else {
    icon?.classList.add("trFlat");
    if (icon) icon.innerHTML = flSvg;
    if (text) text.textContent = "STABIL";
    if (sub) sub.textContent = "STABIL";
  }
}
function updateEstimationUI(levelM, netLps){
  const lvl = clamp(Number(levelM)||0, 0, RES_MAX_M);
  const net = Number(netLps)||0;
  const pct = Math.round((lvl / RES_MAX_M) * 100);

  document.getElementById("est_level_m") && (document.getElementById("est_level_m").textContent = fmt(lvl, 2));
  document.getElementById("est_level_pct") && (document.getElementById("est_level_pct").textContent = String(pct));
  document.getElementById("est_net_lps") && (document.getElementById("est_net_lps").textContent = fmt(net, 2));
  document.getElementById("est_fill") && (document.getElementById("est_fill").style.width = pct + "%");

  setTrendUI(net);

  const etaLabel = document.getElementById("est_eta_label");
  const etaValue = document.getElementById("est_eta_value");

  if (Math.abs(net) < 0.2){
    if (etaLabel) etaLabel.textContent = "ETA";
    if (etaValue) etaValue.textContent = "- (net hampir nol)";
    return;
  }

  if (net < -0.2){
    const eta1 = computeEtaSeconds(lvl, net, 1.0);
    if (etaLabel) etaLabel.textContent = "ETA ke 1m";
    if (etaValue) etaValue.textContent = (eta1 == null) ? "- (tidak menuju 1m)" : secondsToHuman(eta1);
  } else {
    const eta8 = computeEtaSeconds(lvl, net, 8.0);
    if (etaLabel) etaLabel.textContent = "ETA ke 100%";
    if (etaValue) etaValue.textContent = (eta8 == null) ? "- (tidak menuju penuh)" : secondsToHuman(eta8);
  }
}

function applySelisihColor(v){
  const el = document.getElementById("val_SELISIH_FLOW");
  if (!el) return;
  el.classList.remove("valPos","valNeg");
  const n = Number(v);
  if (!isFinite(n)) return;
  if (n > 0) el.classList.add("valPos");
  else if (n < 0) el.classList.add("valNeg");
}

// ===================== APPLY QTY FROM MQTT =====================
let lastQtyTsApplied = 0;

function applyQtyFromMQTT(data){
  const ts = Math.floor(Date.now()/1000);
  const isNew = (ts && ts > lastQtyTsApplied);

  // update lastUpdate label
  const lastUpdate = document.getElementById("lastUpdate");
  if (lastUpdate) lastUpdate.textContent = "LAST UPDATE: " + new Date(ts*1000).toLocaleString();

  // render angka
  for (const [k,v] of Object.entries(data)){
    const el = document.getElementById("val_" + k);
    if (el) el.textContent = fmt(v, 2);
  }

  applySelisihColor(data.SELISIH_FLOW);
  updateReservoir(data.LVL_RES_WTP3, isNew);
  updatePressureGauge(data.PRESSURE_DST, isNew);
  updateEstimationUI(data.LVL_RES_WTP3, data.SELISIH_FLOW);

  // spark tiles
  if (ts){
    const bucket = Math.floor(ts / QTY_TILE_SHIFT_SEC) * QTY_TILE_SHIFT_SEC;
    const label = fmtTime(bucket, true);

    for (const key of qtyTileKeys){
      const v = Number(data[key]);
      if(!isFinite(v)) continue;

      const s = ensureQtySeries(key);
      const isNewBucket = (s.lastBucket !== bucket);

      if (isNewBucket){
        s.labels.push(label);
        s.data.push(v);
        s.lastBucket = bucket;
        while (s.labels.length > QTY_TILE_POINTS){
          s.labels.shift();
          s.data.shift();
        }
      } else {
        if (s.data.length) s.data[s.data.length - 1] = v;
        else { s.labels.push(label); s.data.push(v); }
      }

      const ch = qtyTileCharts[key];
      if (ch){
        ch.options.animation = isNewBucket ? POP_ANIM : { duration: 180, easing: "linear" };
        ch.data.labels = [...s.labels];
        ch.data.datasets[0].data = [...s.data];
        ch.update();
      }
    }

    // big chart current selected param
    const selKey = document.getElementById("qtyParam")?.value || "TOTAL_FLOW_DST";
    const selVal = Number(data[selKey]);
    if (isFinite(selVal)) pushQtyBigPoint(ts, selKey, selVal);
  }

  lastQtyTsApplied = ts;
}

// ==========================================================
// QC: ambil dari CSV Google Sheets (tanpa backend)
// ==========================================================
const qcTileKeys = ["kekeruhan","warna","ph","sisa_chlor"];
const qcTileCharts = {};
const qcTileSeries = {};
let qcBigChart = null;

function ensureQCTileSeries(k){
  if (!qcTileSeries[k]) qcTileSeries[k] = { labels: [], data: [] };
  return qcTileSeries[k];
}

function createQCTileChart(ctx, labels, data){
  return new Chart(ctx, {
    type: "line",
    data: { labels: labels || [], datasets: [{
      data: data || [],
      borderColor: cssVar("--accent"),
      backgroundColor: cssVar("--accentFill"),
      fill: true,
      pointRadius: 0,
      borderWidth: 2,
      tension: 0.35,
    }]},
    options: {
      responsive:true,
      maintainAspectRatio:false,
      animation: POP_ANIM,
      animations: { y: { from: (ctx) => yFromBaseline(ctx) } },
      plugins: { legend:{display:false}, tooltip:{enabled:false} },
      scales: {
        x: { grid: { color: cssVar("--grid"), display:true } },
        y: { grid: { color: cssVar("--grid"), display:true } }
      }
    }
  });
}

function renderQCTile(k){
  const canvas = document.getElementById("qc_spark_" + k);
  if (!canvas) return;
  const s = ensureQCTileSeries(k);

  if (qcTileCharts[k]) qcTileCharts[k].destroy();
  qcTileCharts[k] = createQCTileChart(canvas.getContext("2d"), [...s.labels], [...s.data]);
}

function initQCTileCharts(){
  for (const k of qcTileKeys){
    ensureQCTileSeries(k);
    renderQCTile(k);
  }
}

function qcLabel(k){
  const map = {kekeruhan:"KEKERUHAN", warna:"WARNA", ph:"PH", sisa_chlor:"SISA CHLOR"};
  return map[k] || k;
}

function parseCSV(text){
  // parser CSV sederhana (cukup buat google publish csv)
  const lines = text.split(/\r?\n/).filter(x => x.trim() !== "");
  if (!lines.length) return { headers: [], rows: [] };

  const splitLine = (line) => {
    const out = [];
    let cur = "";
    let inQ = false;
    for (let i=0;i<line.length;i++){
      const ch = line[i];
      if (ch === '"'){ inQ = !inQ; continue; }
      if (ch === "," && !inQ){
        out.push(cur);
        cur = "";
      } else cur += ch;
    }
    out.push(cur);
    return out.map(s => s.trim());
  };

  const headers = splitLine(lines[0]);
  const rows = [];
  for (let i=1;i<lines.length;i++){
    const cols = splitLine(lines[i]);
    const obj = {};
    headers.forEach((h, idx) => obj[h] = cols[idx] ?? "");
    rows.push(obj);
  }
  return { headers, rows };
}

function toFloat(v){
  if (v == null) return null;
  const s = String(v).trim().replace(",", ".");
  if (!s) return null;
  const n = Number(s);
  return isFinite(n) ? n : null;
}

function pickHeader(headers, candidates){
  const norm = (s) => String(s||"").toLowerCase().replace(/\s+/g,"").replace("\ufeff","");
  const hmap = {};
  headers.forEach(h => hmap[norm(h)] = h);
  for (const c of candidates){
    const k = norm(c);
    if (hmap[k]) return hmap[k];
  }
  // contains match
  for (const c of candidates){
    const k = norm(c);
    for (const hk of Object.keys(hmap)){
      if (hk.includes(k)) return hmap[hk];
    }
  }
  return null;
}

function parseDT(s){
  if (!s) return null;
  const t = String(s).trim();
  // coba Date.parse
  const d = new Date(t);
  if (!isNaN(d.getTime())) return d;
  return null;
}

let qcRows = [];
let qcLastUpdate = "-";
let qcLastChlorUpdate = "-";

async function pullQCOnce(){
  try{
    const text = await fetchTextNoCache(QC_CSV_URL);
    const { headers, rows } = parseCSV(text);
    if (!rows.length) return;

    const dtCol = pickHeader(headers, ["DateTime","Datetime","DATE TIME","Date Time","Tanggal","Waktu"]);
    const kekCol = pickHeader(headers, ["Kekeruhan"]);
    const warCol = pickHeader(headers, ["Warna"]);
    const phCol  = pickHeader(headers, ["pH","PH"]);
    const chlCol = pickHeader(headers, ["Sisa Chlor","SisaChlor","Chlor"]);

    const parsed = [];
    for (const r of rows){
      const dtObj = parseDT(dtCol ? r[dtCol] : null);
      if (!dtObj) continue;
      const ts = Math.floor(dtObj.getTime()/1000);
      parsed.push({
        ts,
        dt: dtObj.toLocaleString(),
        kekeruhan: toFloat(kekCol ? r[kekCol] : null),
        warna: toFloat(warCol ? r[warCol] : null),
        ph: toFloat(phCol ? r[phCol] : null),
        sisa_chlor: toFloat(chlCol ? r[chlCol] : null),
      });
    }
    parsed.sort((a,b)=>a.ts-b.ts);
    qcRows = parsed;

    // update latest per param
    const latest = {};
    for (const k of qcTileKeys){
      latest[k] = { ts: null, dt: "-", value: null };
      for (let i=qcRows.length-1;i>=0;i--){
        if (qcRows[i][k] != null){
          latest[k] = { ts: qcRows[i].ts, dt: qcRows[i].dt, value: qcRows[i][k] };
          break;
        }
      }
    }

    // last update label
    qcLastUpdate = latest.kekeruhan.ts || latest.warna.ts || latest.ph.ts
      ? new Date(Math.max(latest.kekeruhan.ts||0, latest.warna.ts||0, latest.ph.ts||0)*1000).toLocaleString()
      : "-";
    qcLastChlorUpdate = latest.sisa_chlor.ts ? new Date(latest.sisa_chlor.ts*1000).toLocaleString() : "-";

    // apply to UI
    const hint = document.getElementById("qcHint");
    if (hint){
      hint.textContent = `QC LAST UPDATE: ${qcLastUpdate} | CHL: ${qcLastChlorUpdate}`;
    }

    for (const k of qcTileKeys){
      const vEl = document.getElementById("qc_val_" + k);
      const dEl = document.getElementById("qc_dt_" + k);
      const obj = latest[k];
      if (vEl) vEl.textContent = (obj.value == null) ? "-" : fmt(obj.value, 2);
      if (dEl) dEl.textContent = obj.dt || "-";
    }

    // mini sparks (last 5 points)
    for (const k of qcTileKeys){
      const pts = qcRows.filter(r => r[k] != null).slice(-5);
      const s = ensureQCTileSeries(k);
      s.labels = pts.map(p => fmtTime(p.ts,false));
      s.data = pts.map(p => p[k]);
      renderQCTile(k);
    }

    // big QC chart
    loadQCBig(true);

  }catch(e){
    console.log("QC pull error", e);
  }
}

function ensureQCBigChart(){
  const canvas = document.getElementById("qcBig");
  if (!canvas || !window.Chart) return;
  const ctx = canvas.getContext("2d");
  if (qcBigChart) return;

  qcBigChart = new Chart(ctx, {
    type:"line",
    data:{ labels: [], datasets:[{
      label: "QC",
      data: [],
      borderColor: cssVar("--accent"),
      backgroundColor: cssVar("--accentFill"),
      fill:true, pointRadius:0, borderWidth:2, tension:0.30
    }]},
    options:{
      responsive:true,
      maintainAspectRatio:false,
      animation: POP_ANIM,
      animations: { y: { from: (ctx) => yFromBaseline(ctx) } },
      scales: {
        x: { grid: { color: cssVar("--grid"), display:true } },
        y: { grid: { color: cssVar("--grid"), display:true } }
      }
    }
  });
}

function loadQCBig(animate=true){
  ensureQCBigChart();
  if (!qcBigChart) return;

  const param = document.getElementById("qcParam")?.value || "kekeruhan";
  const hours = Number(document.getElementById("qcRange")?.value || 24);

  const now = Math.floor(Date.now()/1000);
  const start = now - Math.floor(hours*3600);

  const pts = qcRows.filter(r => r.ts >= start && r[param] != null);
  qcBigChart.data.labels = pts.map(p => new Date(p.ts*1000).toLocaleString());
  qcBigChart.data.datasets[0].data = pts.map(p => p[param]);
  qcBigChart.data.datasets[0].label = `${qcLabel(param)} - ${hours<=24 ? hours+" JAM" : (hours/24)+" HARI"}`;

  qcBigChart.options.animation = animate ? POP_ANIM : false;
  qcBigChart.update();
}

// ===================== MQTT CONNECT =====================
let mqttClient = null;
let wsTryIndex = 0;

function parsePayload(raw){
  // normalisasi mirip python kamu
  if (raw && typeof raw === "object"){
    if (raw.data && typeof raw.data === "object") raw = raw.data;
    else if (raw.payload && typeof raw.payload === "object") raw = raw.payload;
    else if (raw.payload && typeof raw.payload === "string"){
      try{
        const j2 = JSON.parse(raw.payload);
        if (j2 && typeof j2 === "object") raw = j2;
      }catch{}
    }
  }
  if (!raw || typeof raw !== "object") return null;

  const up = {};
  for (const [k,v] of Object.entries(raw)){
    up[String(k).toUpperCase()] = v;
  }
  return up;
}

function connectMQTTNext(){
  if (wsTryIndex >= WS_CANDIDATES.length){
    console.log("MQTT WS: semua kandidat gagal. Cek port/path WS broker.");
    const lastUpdate = document.getElementById("lastUpdate");
    if (lastUpdate) lastUpdate.textContent = "LAST UPDATE: MQTT WS TIDAK CONNECT (cek port)";
    return;
  }

  const url = WS_CANDIDATES[wsTryIndex++];
  console.log("MQTT WS try:", url);

  const lastUpdate = document.getElementById("lastUpdate");
  if (lastUpdate) lastUpdate.textContent = "CONNECTING WS: " + url;

  let connected = false;

  try{
    mqttClient = mqtt.connect(url, {
      clientId: "dash_" + Math.random().toString(16).slice(2),
      keepalive: 60,
      reconnectPeriod: 0, // biar kita pindah kandidat
      connectTimeout: 7000,
      clean: true,
    });
  }catch(e){
    console.log("mqtt.connect error", e);
    connectMQTTNext();
    return;
  }

  mqttClient.on("connect", () => {
    connected = true;
    console.log("MQTT WS connected:", url);
    if (lastUpdate) lastUpdate.textContent = "CONNECTED: " + url;

    mqttClient.subscribe(TOPIC, { qos: 0 }, (err) => {
      if (err) console.log("subscribe err", err);
      else console.log("subscribed", TOPIC);
    });
  });

  mqttClient.on("message", (topic, payload) => {
    try{
      const txt = payload.toString();
      let raw = JSON.parse(txt);
      const up = parsePayload(raw);
      if (!up) return;

      // ambil numeric keys
      const data = {};
      let matched = 0;
      for (const k of NUMERIC_KEYS){
        if (k in up){
          let v = up[k];
          if (typeof v === "string") v = v.trim().replace(",", ".");
          const n = Number(v);
          if (isFinite(n)){
            data[k] = n;
            matched++;
          }
        }
      }
      if (matched === 0) return;

      // derived
      data.SELISIH_FLOW = Number(data.TOTAL_FLOW_ITK||0) - Number(data.TOTAL_FLOW_DST||0);

      applyQtyFromMQTT(data);

    }catch(e){
      console.log("bad mqtt payload", e);
    }
  });

  mqttClient.on("error", (err) => {
    console.log("MQTT error", url, err?.message || err);
    try { mqttClient.end(true); } catch {}
    if (!connected) connectMQTTNext();
  });

  mqttClient.on("close", () => {
    console.log("MQTT close", url);
    if (!connected) connectMQTTNext();
  });
}

// ===================== REDRAW =====================
function redrawAllCharts(){
  setChartDefaults();

  // rebuild tiles qty
  for (const key of qtyTileKeys){
    if (qtyTileCharts[key]) qtyTileCharts[key].destroy();
    qtyTileCharts[key] = null;
    renderQtyTile(key);
  }

  // rebuild tiles qc
  for (const k of qcTileKeys){
    if (qcTileCharts[k]) qcTileCharts[k].destroy();
    qcTileCharts[k] = null;
    renderQCTile(k);
  }

  // gauge+reservoir from current UI
  const p = parseFloat(document.getElementById("val_PRESSURE_DST")?.textContent || "0") || 0;
  const lvl = parseFloat(document.getElementById("val_LVL_RES_WTP3")?.textContent || "0") || 0;
  updatePressureGauge(p, false);
  updateReservoir(lvl, false);

  // big charts refresh
  loadQCBig(false);
}

// ===================== INIT =====================
(async function(){
  try{
    initTheme();
    setChartDefaults();
    buildReservoirLabels();

    setSlide(0);
    initSlidePause();
    setupNavButtons();
    startAutoSlide();

    initQtyTileCharts();
    initQCTileCharts();
    ensureQtyBigChart();
    ensureQCBigChart();

    // events dropdown update
    document.getElementById("qtyParam")?.addEventListener("change", () => {
      // reset big series biar label nyambung
      qtyBigSeries = { labels: [], data: [], lastBucket: null };
    });
    document.getElementById("qcParam")?.addEventListener("change", () => loadQCBig(true));
    document.getElementById("qcRange")?.addEventListener("change", () => loadQCBig(true));

    // start QC polling
    await pullQCOnce();
    setInterval(pullQCOnce, QC_PULL_INTERVAL_MS);

    // start MQTT
    connectMQTTNext();

  }catch(e){
    console.log("INIT FATAL", e);
  }
})();
