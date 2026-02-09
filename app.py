import sqlite3
import paho.mqtt.client as mqtt
import json
import threading
import time
import requests
import csv
import io
import os
from datetime import datetime
from flask import Flask, render_template, jsonify, request, Response

# ================== KONFIGURASI ==================
BROKER = "103.217.145.168"
PORT = 1883
TOPIC = "data/sctkiotserver/groupsctkiotserver/123"

WEB_APP_URL = "https://script.google.com/macros/s/AKfycbzWJVmsuj6p0-JKzksnPcdRkfH0NKa9n0iI_HP2OBaVHbxNQqYaSDGkzbdSraE0sFg-/exec"
SEND_INTERVAL = 60  # detik

QC_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSMKrU7GU9pisN4ihKgSqyC1bDuT1ia6kp-vKWrdUhvaPyX95ZqOBOFy8iBCpQieizqTBJ3R4wNmRII/pub?gid=2046456175&single=true&output=csv"
QC_PULL_INTERVAL = 20  # detik

SCHEDULE_JSON_FILE = "jadwal_2026.json"
SCHEDULE_RELOAD_INTERVAL = 10  # detik

RES_MAX_M = 8.0
RES_TOTAL_M3 = 3000.0
RES_LITER_PER_M = (RES_TOTAL_M3 * 1000.0) / RES_MAX_M  # 375000 L per 1 meter

NUMERIC_KEYS = [
    "PRESSURE_DST","LVL_RES_WTP3","TOTAL_FLOW_ITK","TOTAL_FLOW_DST",
    "FLOW_WTP3","FLOW_50_WTP1","FLOW_CIJERUK","FLOW_CARENANG",
]
DERIVED_KEYS = ["SELISIH_FLOW"]

DISPLAY_ORDER = [
    "TOTAL_FLOW_ITK","TOTAL_FLOW_DST","SELISIH_FLOW","FLOW_WTP3",
    "FLOW_50_WTP1","FLOW_CIJERUK","FLOW_CARENANG",
]

TITLE_MAP = {
    "PRESSURE_DST": "PRESSURE DISTRIBUSI",
    "LVL_RES_WTP3": "LEVEL RESERVOIR WTP 3",
    "TOTAL_FLOW_ITK": "TOTAL FLOW INTAKE",
    "TOTAL_FLOW_DST": "TOTAL FLOW DISTRIBUSI",
    "SELISIH_FLOW": "SELISIH TOTAL FLOW (INTAKE - DISTRIBUSI)",
    "FLOW_WTP3": "FLOW WTP 3",
    "FLOW_50_WTP1": "FLOW UPAM CIKANDE",
    "FLOW_CIJERUK": "FLOW UPAM CIJERUK",
    "FLOW_CARENANG": "FLOW UPAM CARENANG",
}
UNIT_MAP = {
    "PRESSURE_DST": "BAR",
    "LVL_RES_WTP3": "M",
    "TOTAL_FLOW_ITK": "LPS",
    "TOTAL_FLOW_DST": "LPS",
    "SELISIH_FLOW": "LPS",
    "FLOW_WTP3": "LPS",
    "FLOW_50_WTP1": "LPS",
    "FLOW_CIJERUK": "LPS",
    "FLOW_CARENANG": "LPS",
}

QC_PARAMS = {
    "kekeruhan": {"label": "KEKERUHAN", "col": "Kekeruhan", "unit": "NTU"},
    "warna": {"label": "WARNA", "col": "Warna", "unit": "TCU"},
    "ph": {"label": "PH", "col": "pH", "unit": ""},
    "sisa_chlor": {"label": "SISA CHLOR", "col": "Sisa Chlor", "unit": "MG/L"},
}
QC_ORDER = ["kekeruhan", "warna", "ph", "sisa_chlor"]

# ================== GLOBAL ==================
DB_PATH = "history.db"
db_lock = threading.Lock()
data_lock = threading.Lock()

DEFAULT_DATA = {k: 0.0 for k in (NUMERIC_KEYS + DERIVED_KEYS)}
latest_data = DEFAULT_DATA.copy()
latest_ts_epoch = 0
last_send_time = 0.0

qc_lock = threading.Lock()
qc_rows = []
qc_latest = {p: {"ts": None, "dt": "-", "value": None} for p in QC_ORDER}
qc_last_update_dt = "-"
qc_last_update_chlor_dt = "-"
qc_status = {"last_success_dt": "-", "last_error": None, "row_count": 0, "headers": []}

schedule_lock = threading.Lock()
schedule_rows = []
schedule_last_loaded = "-"
schedule_last_error = None
_schedule_mtime = None

# ================== DB ==================
def init_db():
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS measurements (
                ts INTEGER NOT NULL,
                key TEXT NOT NULL,
                value REAL NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_measurements_key_ts ON measurements(key, ts)")
        conn.commit()

def save_to_db(ts_epoch: int, data: dict):
    with db_lock:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            cur = conn.cursor()
            rows = [(ts_epoch, k, float(v)) for k, v in data.items()]
            cur.executemany("INSERT INTO measurements(ts, key, value) VALUES (?, ?, ?)", rows)
            conn.commit()

# ================== QC helpers ==================
def _to_float(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    s = s.replace('"', "").strip().replace(",", ".")
    try:
        return float(s)
    except:
        return None

def _parse_dt(s):
    if not s:
        return None
    s = str(s).strip().replace('"', "")
    fmts = ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except:
            pass
    return None

def _norm_header(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace("\ufeff", "").replace("\n", " ").replace("\r", " ").strip().lower().replace(" ", "")

def _find_col(fieldnames, candidates):
    if not fieldnames:
        return None
    norm_map = {_norm_header(h): h for h in fieldnames}
    for c in candidates:
        k = _norm_header(c)
        if k in norm_map:
            return norm_map[k]
    for c in candidates:
        k = _norm_header(c)
        for nk, orig in norm_map.items():
            if k and k in nk:
                return orig
    return None

def pull_qc_csv_once():
    global qc_rows, qc_latest, qc_last_update_dt, qc_last_update_chlor_dt, qc_status
    try:
        sep = "&" if "?" in QC_CSV_URL else "?"
        url = QC_CSV_URL + f"{sep}_={int(time.time())}"

        r = requests.get(url, timeout=25, allow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0 (QC-Dashboard)"})
        r.raise_for_status()

        f = io.StringIO(r.text)
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        qc_status["headers"] = fieldnames

        dt_col = _find_col(fieldnames, ["DateTime", "Datetime", "DATE TIME", "Date Time"])
        kek_col = _find_col(fieldnames, ["Kekeruhan"])
        war_col = _find_col(fieldnames, ["Warna"])
        ph_col  = _find_col(fieldnames, ["pH", "PH"])
        chl_col = _find_col(fieldnames, ["Sisa Chlor", "SisaChlor"])

        rows = []
        for row in reader:
            dt_obj = _parse_dt(row.get(dt_col) if dt_col else None)
            if not dt_obj:
                continue
            ts = int(dt_obj.timestamp())
            rows.append({
                "ts": ts,
                "dt": dt_obj.strftime("%Y-%m-%d %H:%M"),
                "kekeruhan": _to_float(row.get(kek_col)) if kek_col else None,
                "warna": _to_float(row.get(war_col)) if war_col else None,
                "ph": _to_float(row.get(ph_col)) if ph_col else None,
                "sisa_chlor": _to_float(row.get(chl_col)) if chl_col else None,
            })

        rows.sort(key=lambda x: x["ts"])

        latest_map = {p: {"ts": None, "dt": "-", "value": None} for p in QC_ORDER}

        for p in ["kekeruhan", "warna", "ph"]:
            for rr in reversed(rows):
                if rr.get(p) is not None:
                    latest_map[p] = {"ts": rr["ts"], "dt": rr["dt"], "value": rr[p]}
                    break

        last_chlor_dt = "-"
        for rr in reversed(rows):
            if rr.get("sisa_chlor") is not None:
                latest_map["sisa_chlor"] = {"ts": rr["ts"], "dt": rr["dt"], "value": rr["sisa_chlor"]}
                last_chlor_dt = rr["dt"]
                break

        cand = [latest_map["kekeruhan"]["ts"], latest_map["warna"]["ts"], latest_map["ph"]["ts"]]
        cand = [x for x in cand if x is not None]
        last_qc_dt = "-"
        if cand:
            last_qc_ts = max(cand)
            for rr in reversed(rows):
                if rr["ts"] == last_qc_ts:
                    last_qc_dt = rr["dt"]
                    break

        with qc_lock:
            qc_rows[:] = rows
            qc_latest.clear()
            qc_latest.update(latest_map)
            qc_last_update_dt = last_qc_dt
            qc_last_update_chlor_dt = last_chlor_dt

        qc_status["last_success_dt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        qc_status["last_error"] = None
        qc_status["row_count"] = len(rows)

    except Exception as e:
        qc_status["last_error"] = str(e)
        print("[QC] pull error:", e)

def qc_worker():
    pull_qc_csv_once()
    while True:
        time.sleep(QC_PULL_INTERVAL)
        pull_qc_csv_once()

def qc_history(param: str, hours: float, interval: int):
    if param not in QC_PARAMS:
        return []
    now = int(time.time())
    start = now - int(hours * 3600)

    with qc_lock:
        rows = list(qc_rows)

    filtered = [r for r in rows if r["ts"] >= start and r.get(param) is not None]
    if not filtered:
        return []

    buckets = {}
    for r in filtered:
        b = (r["ts"] // interval) * interval
        buckets.setdefault(b, []).append(r[param])

    out = []
    for b in sorted(buckets.keys()):
        vals = buckets[b]
        out.append({"ts": int(b), "value": float(sum(vals) / max(len(vals), 1))})
    return out

# ================== JADWAL helpers ==================
def _ms_to_datestr(ms):
    try:
        dt = datetime.utcfromtimestamp(int(ms) / 1000.0)
        return dt.strftime("%Y-%m-%d")
    except:
        return None

def _load_schedule_file_if_changed(force=False):
    global schedule_rows, schedule_last_loaded, schedule_last_error, _schedule_mtime

    try:
        if not os.path.exists(SCHEDULE_JSON_FILE):
            raise FileNotFoundError(f"File jadwal tidak ditemukan: {SCHEDULE_JSON_FILE}")

        mtime = os.path.getmtime(SCHEDULE_JSON_FILE)
        if (not force) and (_schedule_mtime is not None) and (mtime == _schedule_mtime):
            return

        with open(SCHEDULE_JSON_FILE, "r", encoding="utf-8") as f:
            rows = json.load(f)

        if not isinstance(rows, list):
            raise ValueError("Format jadwal JSON harus list of objects")

        cleaned = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            cleaned.append({
                "tanggal": r.get("tanggal"),
                "nama": r.get("nama"),
                "jabatan": r.get("jabatan"),
                "shift_kode": r.get("shift_kode"),
                "jam_kerja": r.get("jam_kerja"),
                "lokasi": r.get("lokasi"),
                "jam_mulai": r.get("jam_mulai"),
                "jam_selesai": r.get("jam_selesai"),
            })

        with schedule_lock:
            schedule_rows = cleaned
            schedule_last_loaded = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            schedule_last_error = None
            _schedule_mtime = mtime

    except Exception as e:
        with schedule_lock:
            schedule_last_error = str(e)
            schedule_last_loaded = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("[SCHEDULE] load error:", e)

def schedule_worker():
    _load_schedule_file_if_changed(force=True)
    while True:
        time.sleep(SCHEDULE_RELOAD_INTERVAL)
        _load_schedule_file_if_changed(force=False)

def _schedule_for_date(date_str):
    with schedule_lock:
        rows = list(schedule_rows)

    op = []
    lab = []

    for r in rows:
        d = _ms_to_datestr(r.get("tanggal"))
        if d != date_str:
            continue

        nama = (r.get("nama") or "").strip()
        jab = (r.get("jabatan") or "").strip().lower()
        kode = (r.get("shift_kode") or "").strip().upper()
        jam  = (r.get("jam_kerja") or "").strip()
        lokasi = (r.get("lokasi") or "").strip().upper()

        if not r.get("jam_mulai") or not r.get("jam_selesai"):
            continue

        if jab == "operator produksi":
            if lokasi != "WTP3":
                continue
            if "12" not in kode:
                continue
            op.append({"nama": nama, "kode": kode or "-", "jam": jam or "-", "lokasi": "WTP3"})

        elif jab == "analis laboratorium":
            if lokasi != "LAB":
                continue
            lab.append({"nama": nama, "kode": kode or "-", "jam": jam or "-", "lokasi": "LAB"})

    op.sort(key=lambda x: x["nama"])
    lab.sort(key=lambda x: x["nama"])
    return op, lab

# ================== FLASK ==================
app = Flask(__name__)

@app.after_request
def add_no_cache_headers(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@app.route("/")
def index():
    with data_lock:
        data = dict(latest_data)
    return render_template(
        "index.html",
        data=data,
        title_map=TITLE_MAP,
        unit_map=UNIT_MAP,
        display_order=DISPLAY_ORDER,
        RES_MAX_M=RES_MAX_M,
        RES_LITER_PER_M=RES_LITER_PER_M,
    )

# ===== API kuantitas =====
@app.route("/api/latest")
def api_latest():
    with data_lock:
        data = dict(latest_data)
        ts = int(latest_ts_epoch or time.time())
    return jsonify({"ts": ts, "data": data})

@app.route("/api/history/<key>")
def api_history(key):
    hours = float(request.args.get("hours", 24))
    interval = int(request.args.get("interval", 60))
    now = int(time.time())
    start = now - int(hours * 3600)

    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                (CAST(ts / ? AS INTEGER) * ?) AS bucket,
                AVG(value) AS avg_value
            FROM measurements
            WHERE key = ? AND ts >= ?
            GROUP BY bucket
            ORDER BY bucket
        """, (interval, interval, key, start))
        rows = cur.fetchall()

    out = [{"ts": int(r[0]), "value": float(r[1])} for r in rows]

    limit = request.args.get("limit")
    if limit:
        try:
            n = max(1, int(limit))
            out = out[-n:]
        except:
            pass

    return jsonify(out)

# ===== API QC =====
@app.route("/api/qc/latest")
def api_qc_latest():
    with qc_lock:
        payload = {
            "ts": int(time.time()),
            "qc_last_update": qc_last_update_dt,
            "chlor_last_update": qc_last_update_chlor_dt,
            "latest": qc_latest,
            "status": qc_status,
        }
    return jsonify(payload)

@app.route("/api/qc/history/<param>")
def api_qc_history(param):
    hours = float(request.args.get("hours", 24))
    interval = int(request.args.get("interval", 3600))
    return jsonify(qc_history(param, hours=hours, interval=interval))

@app.route("/api/qc/last/<param>")
def api_qc_last(param):
    n = int(request.args.get("n", 5))
    if param not in QC_PARAMS:
        return jsonify([])

    with qc_lock:
        rows = list(qc_rows)

    out = []
    for rr in reversed(rows):
        v = rr.get(param)
        if v is None:
            continue
        out.append({"ts": rr["ts"], "value": float(v)})
        if len(out) >= n:
            break

    out.reverse()
    return jsonify(out)

# ===== API JADWAL =====
@app.route("/api/schedule")
def api_schedule():
    date_str = (request.args.get("date") or "").strip()
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")

    op, lab = _schedule_for_date(date_str)

    with schedule_lock:
        meta = {"loaded_at": schedule_last_loaded, "error": schedule_last_error, "file": SCHEDULE_JSON_FILE}

    return jsonify({"date": date_str, "operator": op, "lab": lab, "meta": meta})

# ===== SSE stream =====
@app.route("/events")
def events():
    def gen():
        last_qc_sent = None
        last_qty_sent = None
        while True:
            time.sleep(2)

            with data_lock:
                qty = {"ts": int(latest_ts_epoch or time.time()), "data": dict(latest_data)}
            with qc_lock:
                qc = {
                    "ts": int(time.time()),
                    "qc_last_update": qc_last_update_dt,
                    "chlor_last_update": qc_last_update_chlor_dt,
                    "latest": dict(qc_latest),
                    "status": dict(qc_status),
                }

            qty_sig = (qty["ts"], qty["data"].get("TOTAL_FLOW_DST"), qty["data"].get("PRESSURE_DST"), qty["data"].get("LVL_RES_WTP3"))
            qc_sig = (qc.get("qc_last_update"), qc.get("chlor_last_update"),
                      qc["latest"].get("kekeruhan", {}).get("value"),
                      qc["latest"].get("sisa_chlor", {}).get("value"))

            send_qty = (qty_sig != last_qty_sent)
            send_qc = (qc_sig != last_qc_sent)

            if send_qty or send_qc:
                msg = {}
                if send_qty:
                    msg["qty"] = qty
                    last_qty_sent = qty_sig
                if send_qc:
                    msg["qc"] = qc
                    last_qc_sent = qc_sig

                yield f"data: {json.dumps(msg)}\n\n"

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return Response(gen(), headers=headers)

# ================== MQTT ==================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(TOPIC, qos=0)
        client.subscribe(TOPIC + "/#", qos=0)
    else:
        print("Failed to connect to MQTT, code:", rc)

def on_message(client, userdata, msg):
    global last_send_time, latest_ts_epoch
    try:
        payload_text = msg.payload.decode(errors="ignore").strip()
        if not payload_text:
            return

        raw = json.loads(payload_text)

        if isinstance(raw, dict):
            if "data" in raw and isinstance(raw["data"], dict):
                raw = raw["data"]
            elif "payload" in raw and isinstance(raw["payload"], dict):
                raw = raw["payload"]
            elif "payload" in raw and isinstance(raw["payload"], str):
                try:
                    j2 = json.loads(raw["payload"])
                    if isinstance(j2, dict):
                        raw = j2
                except:
                    pass

        if not isinstance(raw, dict):
            return

        raw_u = {str(k).upper(): v for k, v in raw.items()}

        with data_lock:
            prev = dict(latest_data)

        data = {}
        matched = 0
        for key in NUMERIC_KEYS:
            if key in raw_u:
                v = raw_u.get(key)
                try:
                    if isinstance(v, str):
                        v = v.strip().replace(",", ".")
                    data[key] = float(v)
                    matched += 1
                except:
                    data[key] = float(prev.get(key, 0.0))
            else:
                data[key] = float(prev.get(key, 0.0))

        if matched == 0:
            return

        data["SELISIH_FLOW"] = float(data.get("TOTAL_FLOW_ITK", 0.0)) - float(data.get("TOTAL_FLOW_DST", 0.0))

        with data_lock:
            latest_data.clear()
            latest_data.update(data)
            latest_ts_epoch = int(time.time())

        save_to_db(latest_ts_epoch, data)

        now = time.time()
        if now - last_send_time >= SEND_INTERVAL:
            try:
                requests.post(WEB_APP_URL, headers={"Content-Type": "application/json"},
                              data=json.dumps(data), timeout=10)
            except Exception as e:
                print("HTTP post error:", e)
            last_send_time = now

    except Exception as e:
        print("MQTT processing error:", e)

def mqtt_thread():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    client.loop_forever()

# ================== MAIN ==================
if __name__ == "__main__":
    init_db()
    threading.Thread(target=mqtt_thread, daemon=True).start()
    threading.Thread(target=qc_worker, daemon=True).start()
    threading.Thread(target=schedule_worker, daemon=True).start()

    port = int(os.environ.get("PORT", "3000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
