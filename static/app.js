// ======= konstanta dari backend (kalau tidak ada, fallback) =======
const RES_MAX_M = Number(window.RES_MAX_M || 8.0);
const RES_LITER_PER_M = Number(window.RES_LITER_PER_M || 375000.0);

const SLIDE_INTERVAL_MS = 10000;
let slideIndex = 0;
let slideTimer = null;
let isSlidePaused = false;

let lastQtyTsApplied = 0;
let lastQCSigApplied = "";

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
async function fetchJSON(url){
  const sep = url.includes("?") ? "&" : "?";
  const u = url + sep + "_=" + Date.now();
  const res = await fetch(u, { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return await res.json();
}

// ===== THEME =====
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

// ===== Slider controls =====
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

// ===== Reservoir labels =====
function buildReservoirLabels(){
  const labels = document.getElementById("lvl_labels");
  const major = document.getElementById("tank_major_lines");
  if (!labels || !major) return;
  labels.innerHTML = "";
  major.innerHTML = "";
  const max = 8;
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

// ===== Pressure Gauge =====
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

// ===== Chart helpers =====
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
// TILE KUANTITAS (spark)
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
        x: {
          ticks: {
            color: cssVar("--muted"),
            font: { size: 10 },
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            minRotation: 0
          },
          title: { display: true, text: "Jam" },
          grid: { color: cssVar("--grid"), display: true }
        },
        y: {
          ticks: { color: cssVar("--muted"), font: { size: 10 }, maxTicksLimit: 6 },
          grid: { color: cssVar("--grid"), display: true }
        }
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

async function initQtyTileHistory(){
  const hours = 1;
  const interval = QTY_TILE_SHIFT_SEC;

  for (const key of qtyTileKeys){
    try{
      const arr = await fetchJSON(`/api/history/${key}?hours=${hours}&interval=${interval}&limit=${QTY_TILE_POINTS}`);
      const s = ensureQtySeries(key);
      s.labels = arr.map(p => fmtTime(p.ts, true));
      s.data   = arr.map(p => p.value);
      if (arr.length){
        s.lastBucket = Math.floor(arr[arr.length-1].ts / QTY_TILE_SHIFT_SEC) * QTY_TILE_SHIFT_SEC;
      } else {
        s.lastBucket = null;
      }
      renderQtyTile(key);
    }catch(e){
      console.log("INIT QTY TILE ERR", key, e);
    }
  }
}

// ===== Big chart kuantitas =====
let qtyChart = null;
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

async function loadQtyBig(animate=true){
  const key = document.getElementById("qtyParam").value;
  const hours = Number(document.getElementById("qtyRange").value);
  const interval = (hours <= 1) ? 60 : (hours <= 12 ? 120 : 300);
  const arr = await fetchJSON(`/api/history/${key}?hours=${hours}&interval=${interval}`);

  const ctx = document.getElementById("chartBig").getContext("2d");
  if (qtyChart) qtyChart.destroy();
  qtyChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: arr.map(p => fmtTime(p.ts, false)),
      datasets: [{
        label: `${qtyLabel(key)} - ${hours} JAM`,
        data: arr.map(p => p.value),
        borderColor: cssVar("--accent"),
        backgroundColor: cssVar("--accentFill"),
        fill: true,
        pointRadius: 0,
        borderWidth: 2,
        tension: 0.30
      }]
    },
    options: {
      responsive:true,
      maintainAspectRatio:false,
      animation: animate ? POP_ANIM : false,
      animations: animate ? { y: { from: (ctx) => yFromBaseline(ctx) } } : {},
      scales: {
        x: { title: { display: true, text: "Jam" }, grid: { color: cssVar("--grid"), display:true } },
        y: { grid: { color: cssVar("--grid"), display:true } }
      }
    }
  });
}

// ===== QC mini sparks =====
const qcTileKeys = ["kekeruhan","warna","ph","sisa_chlor"];
const qcTileCharts = {};
const qcTileSeries = {};

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

async function refreshQCTileCharts(){
  for (const k of qcTileKeys){
    try{
      const arr = await fetchJSON(`/api/qc/last/${k}?n=5`);
      const s = ensureQCTileSeries(k);
      s.labels = arr.map(p => fmtTime(p.ts, false));
      s.data   = arr.map(p => p.value);
      renderQCTile(k);
    }catch(e){
      console.log("QC TILE REFRESH ERR", k, e);
    }
  }
}

// ===== QC big chart =====
let qcBigChart = null;
function qcLabel(k){
  const map = {kekeruhan:"KEKERUHAN", warna:"WARNA", ph:"PH", sisa_chlor:"SISA CHLOR"};
  return map[k] || k;
}
async function loadQCBig(animate=true){
  const param = document.getElementById("qcParam").value;
  const hours = Number(document.getElementById("qcRange").value);
  const interval = (hours <= 24) ? 3600 : (hours <= 168 ? 7200 : 21600);
  const arr = await fetchJSON(`/api/qc/history/${param}?hours=${hours}&interval=${interval}`);

  const ctx = document.getElementById("qcBig").getContext("2d");
  if (qcBigChart) qcBigChart.destroy();
  qcBigChart = new Chart(ctx, {
    type:"line",
    data:{
      labels: arr.map(p => new Date(p.ts*1000).toLocaleString()),
      datasets:[{
        label: `${qcLabel(param)} - ${hours<=24 ? hours+" JAM" : (hours/24)+" HARI"}`,
        data: arr.map(p => p.value),
        borderColor: cssVar("--accent"),
        backgroundColor: cssVar("--accentFill"),
        fill:true, pointRadius:0, borderWidth:2, tension:0.30
      }]
    },
    options:{
      responsive:true,
      maintainAspectRatio:false,
      animation: animate ? POP_ANIM : false,
      animations: animate ? { y: { from: (ctx) => yFromBaseline(ctx) } } : {},
      scales: {
        x: { grid: { color: cssVar("--grid"), display:true } },
        y: { grid: { color: cssVar("--grid"), display:true } }
      }
    }
  });
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

  icon.classList.remove("trUp","trDown","trFlat");

  if (net > 0.2){
    icon.classList.add("trUp");
    icon.innerHTML = upSvg;
    if (text) text.textContent = "CADANGAN NAIK";
    if (sub) sub.textContent = "NAIK";
  } else if (net < -0.2){
    icon.classList.add("trDown");
    icon.innerHTML = dnSvg;
    if (text) text.textContent = "CADANGAN TURUN";
    if (sub) sub.textContent = "TURUN";
  } else {
    icon.classList.add("trFlat");
    icon.innerHTML = flSvg;
    if (text) text.textContent = "STABIL";
    if (sub) sub.textContent = "STABIL";
  }
}

function updateEstimationUI(levelM, netLps){
  const lvl = clamp(Number(levelM)||0, 0, RES_MAX_M);
  const net = Number(netLps)||0;
  const pct = Math.round((lvl / RES_MAX_M) * 100);

  document.getElementById("est_level_m").textContent = fmt(lvl, 2);
  document.getElementById("est_level_pct").textContent = String(pct);
  document.getElementById("est_net_lps").textContent = fmt(net, 2);
  document.getElementById("est_fill").style.width = pct + "%";

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

// ===== Apply data to UI =====
function applyQty(payload){
  if (!payload) return;
  const ts = payload.ts || 0;
  const data = payload.data || {};

  const isNew = (ts && ts > lastQtyTsApplied);

  if (ts) {
    document.getElementById("lastUpdate").textContent =
      "LAST UPDATE: " + new Date(ts*1000).toLocaleString();
  }

  for (const [k,v] of Object.entries(data)){
    const id = "val_" + k;
    const el = document.getElementById(id);
    if (el) el.textContent = fmt(v, 2);
  }

  applySelisihColor(data.SELISIH_FLOW);

  updateReservoir(data.LVL_RES_WTP3, isNew);
  updatePressureGauge(data.PRESSURE_DST, isNew);

  updateEstimationUI(data.LVL_RES_WTP3, data.SELISIH_FLOW);

  if (ts){
    const bucket = Math.floor(ts / 10) * 10;
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
        while (s.labels.length > 18){
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
      } else {
        renderQtyTile(key);
      }
    }
  }

  if (isNew){
    loadQtyBig(true);
    lastQtyTsApplied = ts;
  }
}

function applyQC(payload){
  if (!payload) return;

  document.getElementById("qcHint").textContent =
    `QC LAST UPDATE: ${payload.qc_last_update || "-"} | CHL: ${payload.chlor_last_update || "-"}`;

  const sig = JSON.stringify({
    u: payload.qc_last_update,
    c: payload.chlor_last_update,
    k: payload.latest?.kekeruhan?.value,
    s: payload.latest?.sisa_chlor?.value
  });
  const isNewQC = (sig !== lastQCSigApplied);

  for (const k of qcTileKeys){
    const vEl = document.getElementById("qc_val_" + k);
    const dEl = document.getElementById("qc_dt_" + k);
    const obj = (payload.latest && payload.latest[k]) ? payload.latest[k] : null;

    const val = (obj && obj.value != null) ? obj.value : null;
    if (vEl){
      vEl.textContent = (val == null) ? "-" : fmt(val, 2);
    }
    if (dEl) dEl.textContent = (obj && obj.dt) ? obj.dt : "-";
  }

  if (isNewQC){
    lastQCSigApplied = sig;
    refreshQCTileCharts();
    loadQCBig(true);
  }
}

// ===== JADWAL (Slide QC) =====
function ymd(d){
  const pad = (n)=> String(n).padStart(2,"0");
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
}
function setSchDateInput(val){
  const inp = document.getElementById("schDate");
  if (inp) inp.value = val;
}
function renderScheduleRows(tbodyId, rows){
  const tb = document.getElementById(tbodyId);
  if (!tb) return;
  tb.innerHTML = "";
  if (!rows || !rows.length){
    tb.innerHTML = `<tr><td class="emptyRow" colspan="3">- TIDAK ADA DATA -</td></tr>`;
    return;
  }
  for (const r of rows){
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${(r.nama||"-")}</td>
      <td>${(r.jam||"-")}</td>
      <td class="lokasi">${(r.lokasi||"-")}</td>
    `;
    tb.appendChild(tr);
  }
}
async function loadSchedule(dateStr){
  try{
    const j = await fetchJSON(`/api/schedule?date=${encodeURIComponent(dateStr)}`);
    renderScheduleRows("schBodyOp", j.operator || []);
    renderScheduleRows("schBodyLab", j.lab || []);
  }catch(e){
    renderScheduleRows("schBodyOp", []);
    renderScheduleRows("schBodyLab", []);
    console.log("SCHEDULE LOAD ERR", e);
  }
}
function initSchedule(){
  const today = new Date();
  const d0 = ymd(today);
  setSchDateInput(d0);
  loadSchedule(d0);

  const btnT = document.getElementById("schToday");
  const btnB = document.getElementById("schTomorrow");
  const inp  = document.getElementById("schDate");

  if (btnT) btnT.addEventListener("click", () => {
    const d = ymd(new Date());
    setSchDateInput(d);
    loadSchedule(d);
  });
  if (btnB) btnB.addEventListener("click", () => {
    const x = new Date();
    x.setDate(x.getDate()+1);
    const d = ymd(x);
    setSchDateInput(d);
    loadSchedule(d);
  });
  if (inp) inp.addEventListener("change", () => {
    const d = inp.value;
    if (d) loadSchedule(d);
  });

  // refresh tiap 5 menit
  setInterval(() => {
    const d = (document.getElementById("schDate")?.value) || ymd(new Date());
    loadSchedule(d);
  }, 300000);
}

// ===== SSE =====
function startSSE(){
  try{
    const es = new EventSource("/events");
    es.onmessage = async (ev) => {
      try{
        const j = JSON.parse(ev.data);
        if (j.qty) applyQty(j.qty);
        if (j.qc) applyQC(j.qc);
      }catch(e){
        console.log("SSE parse/apply error", e);
      }
    };
    es.onerror = () => {
      console.log("SSE error, fallback polling...");
      es.close();
      startPolling();
    };
    return true;
  }catch(e){
    console.log("SSE not available", e);
    return false;
  }
}

// ===== Polling fallback =====
let pollTimer = null;
function startPolling(){
  if (pollTimer) return;
  pollTimer = setInterval(async () => {
    try{
      const qty = await fetchJSON("/api/latest");
      applyQty(qty);
    }catch(e){}

    try{
      const qc = await fetchJSON("/api/qc/latest");
      applyQC(qc);
    }catch(e){}
  }, 5000);
}

function redrawAllCharts(){
  setChartDefaults();

  for (const key of qtyTileKeys){
    if (qtyTileCharts[key]) qtyTileCharts[key].destroy();
    qtyTileCharts[key] = null;
    renderQtyTile(key);
  }

  for (const k of qcTileKeys){
    if (qcTileCharts[k]) qcTileCharts[k].destroy();
    qcTileCharts[k] = null;
    renderQCTile(k);
  }

  const p = parseFloat(document.getElementById("val_PRESSURE_DST")?.textContent || "0") || 0;
  const lvl = parseFloat(document.getElementById("val_LVL_RES_WTP3")?.textContent || "0") || 0;
  updatePressureGauge(p, false);
  updateReservoir(lvl, false);

  loadQtyBig(false);
  loadQCBig(false);
}

// ======== INIT ========
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

    await initQtyTileHistory();
    await refreshQCTileCharts();

    await loadQtyBig(false);
    await loadQCBig(false);

    document.getElementById("qtyParam").addEventListener("change", () => loadQtyBig(true));
    document.getElementById("qtyRange").addEventListener("change", () => loadQtyBig(true));
    document.getElementById("qcParam").addEventListener("change", () => loadQCBig(true));
    document.getElementById("qcRange").addEventListener("change", () => loadQCBig(true));

    try{ applyQty(await fetchJSON("/api/latest")); }catch(e){}
    try{ applyQC(await fetchJSON("/api/qc/latest")); }catch(e){}

    initSchedule();

    if (!startSSE()) startPolling();

  }catch(e){
    console.log("INIT FATAL", e);
    startPolling();
  }
})();
