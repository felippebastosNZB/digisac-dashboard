"""
Digisac Dashboard — Backend com Webhook
"""
import os, json
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
from collections import defaultdict
import sqlite3
import threading

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

DB_PATH = os.path.join(os.path.dirname(__file__), 'tickets.db')
db_lock = threading.Lock()

CONSULTORES = {
    "17573484-0c14-4507-bb51-e8dec30e4e90": "Aline Lourenço",
    "c525a3b3-fcb6-4097-8e81-3a4a727ab934": "Cris Paiva",
    "6e6726fd-16b6-44da-af14-21e1acb654cb": "Glaucia Silva",
    "bab0e0c4-0c1e-4c6d-be3d-7d315b4a9bea": "Jaqueline dos Santos",
    "8287c130-11d7-4688-94e2-8bfe0574ca8c": "Kely Melo Soares",
    "0bda91fc-1275-4d9d-9835-37381a09a2aa": "Pedro Leão",
}

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS tickets (
            id TEXT PRIMARY KEY,
            protocol TEXT,
            user_id TEXT,
            user_name TEXT,
            contact_name TEXT,
            department_name TEXT,
            is_open INTEGER,
            started_at TEXT,
            ended_at TEXT,
            ticket_time REAL,
            waiting_time REAL,
            messaging_time REAL,
            transfer_count INTEGER,
            raw_event TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS webhook_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            payload TEXT,
            received_at TEXT DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()

init_db()

def extract_ticket(data):
    ticket = data.get('ticket') or data.get('data') or data
    if not ticket or not isinstance(ticket, dict):
        return None

    tid = ticket.get('id')
    if not tid:
        return None

    metrics = ticket.get('metrics') or {}
    user = ticket.get('user') or {}
    contact = ticket.get('contact') or {}
    department = ticket.get('department') or {}

    user_id   = ticket.get('userId') or user.get('id', '')
    user_name = ticket.get('userName') or user.get('name', '')
    if not user_name and user_id in CONSULTORES:
        user_name = CONSULTORES[user_id]

    return {
        'id':              tid,
        'protocol':        ticket.get('protocol', ''),
        'user_id':         user_id,
        'user_name':       user_name,
        'contact_name':    ticket.get('contactName') or contact.get('name', ''),
        'department_name': ticket.get('departmentName') or department.get('name', ''),
        'is_open':         1 if ticket.get('isOpen', True) else 0,
        'started_at':      ticket.get('startedAt') or ticket.get('createdAt', ''),
        'ended_at':        ticket.get('endedAt') or ticket.get('closedAt', ''),
        'ticket_time':     (metrics.get('ticketTime') or 0) / 60,
        'waiting_time':    (metrics.get('waitingTime') or 0) / 60,
        'messaging_time':  (metrics.get('messagingTime') or 0) / 60,
        'transfer_count':  metrics.get('ticketTransferCount') or 0,
    }

def upsert_ticket(t):
    with db_lock:
        with get_db() as conn:
            conn.execute('''INSERT INTO tickets
                (id, protocol, user_id, user_name, contact_name, department_name,
                 is_open, started_at, ended_at, ticket_time, waiting_time,
                 messaging_time, transfer_count, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    is_open=excluded.is_open,
                    ended_at=excluded.ended_at,
                    ticket_time=excluded.ticket_time,
                    waiting_time=excluded.waiting_time,
                    messaging_time=excluded.messaging_time,
                    transfer_count=excluded.transfer_count,
                    updated_at=CURRENT_TIMESTAMP
            ''', (t['id'], t['protocol'], t['user_id'], t['user_name'],
                  t['contact_name'], t['department_name'], t['is_open'],
                  t['started_at'], t['ended_at'], t['ticket_time'],
                  t['waiting_time'], t['messaging_time'], t['transfer_count']))
            conn.commit()

# ── Webhook endpoint ────────────────────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        payload = request.get_json(force=True, silent=True) or {}
        event_type = payload.get('event') or payload.get('type') or 'unknown'

        # loga o evento
        with db_lock:
            with get_db() as conn:
                conn.execute('INSERT INTO webhook_log (event_type, payload) VALUES (?,?)',
                             (event_type, json.dumps(payload)))
                conn.commit()

        # processa ticket
        t = extract_ticket(payload)
        if t and t['user_id'] in CONSULTORES:
            upsert_ticket(t)

        return jsonify({'ok': True}), 200
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 200

# ── API de dados ────────────────────────────────────────────────────────────
@app.route('/api/data')
def data():
    date_from = request.args.get('from', '2026-03-01')
    date_to   = request.args.get('to',   '2026-03-31')

    date_from_dt = date_from + 'T00:00:00'
    date_to_dt   = date_to   + 'T23:59:59'

    with get_db() as conn:
        rows = conn.execute('''
            SELECT * FROM tickets
            WHERE started_at >= ? AND started_at <= ?
            AND user_id IN ({})
        '''.format(','.join('?'*len(CONSULTORES))),
        [date_from_dt, date_to_dt] + list(CONSULTORES.keys())).fetchall()

    tickets = [dict(r) for r in rows]

    # agrupa por consultor
    stats = {}
    for uid, nome in CONSULTORES.items():
        stats[nome] = {
            'nome': nome,
            'open': 0, 'closed': 0, 'total': 0,
            'ticket_times': [], 'waiting_times': [],
            'messaging_times': [], 'transfers': [],
            'daily': defaultdict(int),
        }

    for t in tickets:
        uid  = t['user_id']
        nome = CONSULTORES.get(uid)
        if not nome: continue
        s = stats[nome]
        if t['is_open']:
            s['open'] += 1
        else:
            s['closed'] += 1
            if t['ticket_time']:    s['ticket_times'].append(t['ticket_time'])
            if t['waiting_time']:   s['waiting_times'].append(t['waiting_time'])
            if t['messaging_time']: s['messaging_times'].append(t['messaging_time'])
            if t['transfer_count'] is not None:
                s['transfers'].append(t['transfer_count'])
        s['total'] += 1
        day = (t['started_at'] or '')[:10]
        if day: s['daily'][day] += 1

    def avg(lst): return round(sum(lst)/len(lst), 1) if lst else 0

    result = []
    for nome, s in stats.items():
        result.append({
            'nome':               nome,
            'open':               s['open'],
            'closed':             s['closed'],
            'total':              s['total'],
            'avg_ticket_time':    avg(s['ticket_times']),
            'avg_waiting_time':   avg(s['waiting_times']),
            'avg_messaging_time': avg(s['messaging_times']),
            'avg_transfers':      avg(s['transfers']),
            'daily':              dict(sorted(s['daily'].items())),
        })

    # score ranking
    max_total = max((c['total'] for c in result), default=1) or 1
    max_wait  = max((c['avg_waiting_time'] for c in result), default=1) or 1
    max_conv  = max((c['avg_messaging_time'] for c in result), default=1) or 1
    for c in result:
        score_vol  = (c['total'] / max_total) * 40
        score_wait = ((max_wait - c['avg_waiting_time']) / max_wait) * 35 if max_wait else 0
        score_conv = ((max_conv - c['avg_messaging_time']) / max_conv) * 25 if max_conv else 0
        c['score'] = round(score_vol + score_wait + score_conv, 1)
    result.sort(key=lambda x: -x['score'])
    for i, c in enumerate(result): c['rank'] = i + 1

    # diário geral
    daily_all = defaultdict(int)
    for c in result:
        for day, count in c['daily'].items():
            daily_all[day] += count

    totais = {
        'total_tickets':      sum(c['total'] for c in result),
        'total_open':         sum(c['open'] for c in result),
        'total_closed':       sum(c['closed'] for c in result),
        'avg_waiting_time':   avg([c['avg_waiting_time'] for c in result if c['avg_waiting_time']]),
        'avg_messaging_time': avg([c['avg_messaging_time'] for c in result if c['avg_messaging_time']]),
        'avg_ticket_time':    avg([c['avg_ticket_time'] for c in result if c['avg_ticket_time']]),
    }

    return jsonify({
        'consultores': result,
        'totais': totais,
        'daily': [{'date': k, 'count': v} for k, v in sorted(daily_all.items())],
        'total_stored': len(tickets),
        'period': {'from': date_from, 'to': date_to},
    })

@app.route('/api/webhook-log')
def webhook_log():
    with get_db() as conn:
        rows = conn.execute('SELECT * FROM webhook_log ORDER BY received_at DESC LIMIT 50').fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/stats')
def stats():
    with get_db() as conn:
        total = conn.execute('SELECT COUNT(*) as n FROM tickets').fetchone()['n']
        logs  = conn.execute('SELECT COUNT(*) as n FROM webhook_log').fetchone()['n']
        last  = conn.execute('SELECT MAX(updated_at) as t FROM tickets').fetchone()['t']
    return jsonify({'total_tickets': total, 'total_webhooks': logs, 'last_update': last})

@app.route('/')
def index():
    with open(os.path.join(os.path.dirname(__file__), 'static', 'index.html'), 'r', encoding='utf-8') as f:
        return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}

if __name__ == '__main__':
    print("Dashboard rodando em http://localhost:5000")
    print(f"URL do webhook para configurar no Digisac:")
    print(f"  https://SEU-DOMINIO.onrender.com/webhook")
    app.run(debug=False, host='0.0.0.0', port=5000)
