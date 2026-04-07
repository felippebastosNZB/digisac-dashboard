"""
Digisac Dashboard — Backend Flask
Instalar: pip install flask flask-cors requests
Rodar: python app.py
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import threading
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

BASE_URL = "https://nzbembalagens.digisac.chat/api/v1"
TOKEN    = "d93b455e5470d399f5e48a93952c8e10902f7cfd"
HEADERS  = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

CONSULTORES = {
    "Aline Lourenço":       "17573484-0c14-4507-bb51-e8dec30e4e90",
    "Cris Paiva":           "c525a3b3-fcb6-4097-8e81-3a4a727ab934",
    "Glaucia Silva":        "6e6726fd-16b6-44da-af14-21e1acb654cb",
    "Jaqueline dos Santos": "bab0e0c4-0c1e-4c6d-be3d-7d315b4a9bea",
    "Kely Melo Soares":     "8287c130-11d7-4688-94e2-8bfe0574ca8c",
    "Pedro Leão":           "0bda91fc-1275-4d9d-9835-37381a09a2aa",
}

# Cache global
cache = {
    "status": "idle",          # idle | loading | ready | error
    "progress": {},            # consultor -> {"loaded": N, "total": N}
    "data": {},                # consultor -> stats
    "last_updated": None,
    "period": None,
}
cache_lock = threading.Lock()

def api_get(endpoint, params):
    try:
        r = requests.get(f"{BASE_URL}{endpoint}", headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return None

def parse_dt(s):
    if not s: return None
    try: return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except: return None

def get_start(t): return t.get("startedAt") or t.get("createdAt")
def get_end(t):   return t.get("endedAt") or t.get("closedAt")

def avg(lst): return round(sum(lst)/len(lst), 1) if lst else 0

def load_consultant(nome, uid, date_from, date_to, is_open):
    """Busca todos os tickets de um consultor no período paginando."""
    items, page = [], 1
    key = f"{nome}_{'open' if is_open else 'closed'}"

    while True:
        data = api_get("/tickets", {
            "userId": uid,
            "isOpen": "true" if is_open else "false",
            "page": page,
            "limit": 100
        })
        if not data: break
        batch = data.get("data", [])
        if not batch: break

        total = data.get("total", 0)
        last  = int(data.get("lastPage", 1))

        with cache_lock:
            cache["progress"][key] = {"loaded": page * 100, "total": total, "last_page": last}

        in_period, before_period = 0, 0
        for t in batch:
            dt = parse_dt(get_start(t))
            if dt is None: continue
            if date_from <= dt <= date_to:
                items.append(t)
                in_period += 1
            elif dt < date_from:
                before_period += 1

        if page >= last: break
        # heurística: se >80% dos tickets desta página são anteriores ao período, para
        if before_period > 0 and in_period == 0 and before_period >= len(batch) * 0.8:
            break
        page += 1

    return items

def compute_stats(nome, t_open, t_closed, dmap):
    ticket_times, waiting_times, messaging_times, transfers = [], [], [], []
    queues = defaultdict(int)
    daily_counts = defaultdict(int)

    for t in t_closed:
        m = t.get("metrics") or {}
        if m.get("ticketTime"):    ticket_times.append(m["ticketTime"] / 60)
        if m.get("waitingTime"):   waiting_times.append(m["waitingTime"] / 60)
        if m.get("messagingTime"): messaging_times.append(m["messagingTime"] / 60)
        if m.get("ticketTransferCount") is not None:
            transfers.append(m["ticketTransferCount"])
        dt = parse_dt(get_start(t))
        if dt: daily_counts[dt.strftime("%Y-%m-%d")] += 1
        did = str(t.get("departmentId",""))
        queues[dmap.get(did, "Outro")] += 1

    for t in t_open:
        dt = parse_dt(get_start(t))
        if dt: daily_counts[dt.strftime("%Y-%m-%d")] += 1
        did = str(t.get("departmentId",""))
        queues[dmap.get(did, "Outro")] += 1

    total = len(t_open) + len(t_closed)
    return {
        "nome": nome,
        "open": len(t_open),
        "closed": len(t_closed),
        "total": total,
        "avg_ticket_time":    avg(ticket_times),
        "avg_waiting_time":   avg(waiting_times),
        "avg_messaging_time": avg(messaging_times),
        "avg_transfers":      avg(transfers),
        "daily_counts":       dict(sorted(daily_counts.items())),
        "queues":             dict(sorted(queues.items(), key=lambda x: -x[1])),
        "top_queue":          max(queues, key=queues.get) if queues else "—",
    }

def run_load(date_from_str, date_to_str):
    """Roda em background thread."""
    with cache_lock:
        cache["status"] = "loading"
        cache["progress"] = {}
        cache["data"] = {}
        cache["period"] = {"from": date_from_str, "to": date_to_str}

    try:
        date_from = datetime.fromisoformat(date_from_str).replace(tzinfo=timezone.utc)
        date_to   = datetime.fromisoformat(date_to_str).replace(tzinfo=timezone.utc)

        # departamentos
        ddata = api_get("/departments", {"limit": 100})
        dmap  = {str(d.get("id","")): d.get("name","?") for d in (ddata.get("data",[]) if ddata else [])}

        results = {}
        for nome, uid in CONSULTORES.items():
            t_open   = load_consultant(nome, uid, date_from, date_to, is_open=True)
            t_closed = load_consultant(nome, uid, date_from, date_to, is_open=False)
            results[nome] = compute_stats(nome, t_open, t_closed, dmap)

        with cache_lock:
            cache["data"]         = results
            cache["status"]       = "ready"
            cache["last_updated"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    except Exception as e:
        with cache_lock:
            cache["status"] = "error"
            cache["error"]  = str(e)

# ── API Routes ─────────────────────────────────────────────────────────────
@app.route("/api/status")
def status():
    with cache_lock:
        return jsonify({
            "status":       cache["status"],
            "progress":     cache["progress"],
            "last_updated": cache["last_updated"],
            "period":       cache["period"],
        })

@app.route("/api/load", methods=["POST"])
def load():
    body      = request.json or {}
    date_from = body.get("from", "2026-03-01T00:00:00")
    date_to   = body.get("to",   "2026-03-31T23:59:59")
    t = threading.Thread(target=run_load, args=(date_from, date_to), daemon=True)
    t.start()
    return jsonify({"ok": True})

@app.route("/api/data")
def data():
    with cache_lock:
        d = cache["data"]
        s = cache["status"]
        lu = cache["last_updated"]
        p  = cache["period"]
    if not d:
        return jsonify({"status": s, "data": {}})

    consultores_list = []
    for nome, stats in d.items():
        consultores_list.append(stats)

    # ranking: 40% total tickets + 35% tempo conversa (inverso) + 25% tempo espera (inverso)
    max_total = max((c["total"] for c in consultores_list), default=1) or 1
    max_conv  = max((c["avg_messaging_time"] for c in consultores_list), default=1) or 1
    max_wait  = max((c["avg_waiting_time"] for c in consultores_list), default=1) or 1

    for c in consultores_list:
        score_vol  = (c["total"] / max_total) * 40
        score_conv = ((max_conv - c["avg_messaging_time"]) / max_conv) * 35 if max_conv else 0
        score_wait = ((max_wait - c["avg_waiting_time"]) / max_wait) * 25 if max_wait else 0
        c["score"] = round(score_vol + score_conv + score_wait, 1)

    consultores_list.sort(key=lambda x: -x["score"])
    for i, c in enumerate(consultores_list):
        c["rank"] = i + 1

    # totais empresa
    totais = {
        "total_tickets":       sum(c["total"] for c in consultores_list),
        "total_open":          sum(c["open"] for c in consultores_list),
        "total_closed":        sum(c["closed"] for c in consultores_list),
        "avg_waiting_time":    avg([c["avg_waiting_time"] for c in consultores_list if c["avg_waiting_time"]]),
        "avg_messaging_time":  avg([c["avg_messaging_time"] for c in consultores_list if c["avg_messaging_time"]]),
        "avg_ticket_time":     avg([c["avg_ticket_time"] for c in consultores_list if c["avg_ticket_time"]]),
    }

    # evolução diária (todos consultores juntos)
    daily_all = defaultdict(int)
    for c in consultores_list:
        for day, count in c["daily_counts"].items():
            daily_all[day] += count
    daily_sorted = [{"date": k, "count": v} for k, v in sorted(daily_all.items())]

    return jsonify({
        "status":       s,
        "last_updated": lu,
        "period":       p,
        "totais":       totais,
        "consultores":  consultores_list,
        "daily":        daily_sorted,
    })

@app.route("/")
def index():
    return app.send_static_file("index.html")

if __name__ == "__main__":
    print("="*50)
    print("  Digisac Dashboard rodando em:")
    print("  http://localhost:5000")
    print("="*50)
    app.run(debug=False, host="0.0.0.0", port=5000)
