import os, json
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
from collections import defaultdict
import threading
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

DATABASE_URL = os.environ.get('DATABASE_URL', '')
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
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    with db_lock:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS tickets (
            id TEXT PRIMARY KEY,
            protocol TEXT,
            user_id TEXT,
            user_name TEXT,
            contact_name TEXT,
            department_id TEXT,
            is_open BOOLEAN,
            started_at TIMESTAMPTZ,
            ended_at TIMESTAMPTZ,
            ticket_time REAL,
            waiting_time REAL,
            messaging_time REAL,
            transfer_count INTEGER,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS webhook_log (
            id SERIAL PRIMARY KEY,
            event_type TEXT,
            payload TEXT,
            received_at TIMESTAMPTZ DEFAULT NOW()
        )''')
        conn.commit()
        cur.close()
        conn.close()

init_db()

def extract_ticket(payload):
    ticket = payload.get('data') or payload.get('ticket') or payload
    if not ticket or not isinstance(ticket, dict):
        return None
    tid = ticket.get('id')
    if not tid:
        return None
    metrics = ticket.get('metrics') or {}
    contact = ticket.get('contact') or {}
    user_id = ticket.get('userId') or ''
    return {
        'id':            tid,
        'protocol':      ticket.get('protocol', ''),
        'user_id':       user_id,
        'user_name':     CONSULTORES.get(user_id, ''),
        'contact_name':  ticket.get('contactName') or contact.get('name', ''),
        'department_id': ticket.get('departmentId', ''),
        'is_open':       ticket.get('isOpen', True),
        'started_at':    ticket.get('startedAt') or ticket.get('createdAt') or None,
        'ended_at':      ticket.get('endedAt') or None,
        'ticket_time':   (metrics.get('ticketTime') or 0) / 60,
        'waiting_time':  (metrics.get('waitingTime') or 0) / 60,
        'messaging_time':(metrics.get('messagingTime') or 0) / 60,
        'transfer_count':metrics.get('ticketTransferCount') or 0,
    }

def upsert_ticket(t):
    with db_lock:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO tickets
              (id, protocol, user_id, user_name, contact_name, department_id,
               is_open, started_at, ended_at, ticket_time, waiting_time,
               messaging_time, transfer_count, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (id) DO UPDATE SET
              user_id       = CASE WHEN excluded.user_id != '' THEN excluded.user_id ELSE tickets.user_id END,
              user_name     = CASE WHEN excluded.user_name != '' THEN excluded.user_name ELSE tickets.user_name END,
              is_open       = excluded.is_open,
              ended_at      = COALESCE(excluded.ended_at, tickets.ended_at),
              ticket_time   = CASE WHEN excluded.ticket_time > 0 THEN excluded.ticket_time ELSE tickets.ticket_time END,
              waiting_time  = CASE WHEN excluded.waiting_time > 0 THEN excluded.waiting_time ELSE tickets.waiting_time END,
              messaging_time= CASE WHEN excluded.messaging_time > 0 THEN excluded.messaging_time ELSE tickets.messaging_time END,
              transfer_count= excluded.transfer_count,
              updated_at    = NOW()
        ''', (t['id'], t['protocol'], t['user_id'], t['user_name'],
              t['contact_name'], t['department_id'], t['is_open'],
              t['started_at'], t['ended_at'], t['ticket_time'],
              t['waiting_time'], t['messaging_time'], t['transfer_count']))
        conn.commit()
        cur.close()
        conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        payload    = request.get_json(force=True, silent=True) or {}
        event_type = payload.get('event') or payload.get('type') or 'unknown'
        with db_lock:
            conn = get_db()
            cur  = conn.cursor()
            cur.execute('INSERT INTO webhook_log (event_type, payload) VALUES (%s,%s)',
                        (event_type, json.dumps(payload)))
            conn.commit()
            cur.close()
            conn.close()
        t = extract_ticket(payload)
        if t:
            upsert_ticket(t)
        return jsonify({'ok': True}), 200
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 200

@app.route('/api/data')
def data():
    date_from = request.args.get('from', datetime.now().strftime('%Y-%m-%d'))
    date_to   = request.args.get('to',   datetime.now().strftime('%Y-%m-%d'))
    df = date_from + 'T00:00:00+00:00'
    dt = date_to   + 'T23:59:59+00:00'

    conn = get_db()
    cur  = conn.cursor()
    placeholders = ','.join(['%s'] * len(CONSULTORES))
    cur.execute(f'''
        SELECT * FROM tickets
        WHERE started_at >= %s AND started_at <= %s
        AND user_id IN ({placeholders})
    ''', [df, dt] + list(CONSULTORES.keys()))
    tickets = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    stats = {}
    for uid, nome in CONSULTORES.items():
        stats[nome] = {
            'nome': nome, 'open': 0, 'closed': 0, 'total': 0,
            'ticket_times': [], 'waiting_times': [],
            'messaging_times': [], 'transfers': [],
            'daily': defaultdict(int),
        }

    for t in tickets:
        nome = CONSULTORES.get(t['user_id'])
        if not nome: continue
        s = stats[nome]
        if t['is_open']:
            s['open'] += 1
        else:
            s['closed'] += 1
            if t['ticket_time']:    s['ticket_times'].append(t['ticket_time'])
            if t['waiting_time']:   s['waiting_times'].append(t['waiting_time'])
            if t['messaging_time']: s['messaging_times'].append(t['messaging_time'])
            s['transfers'].append(t['transfer_count'] or 0)
        s['total'] += 1
        day = t['started_at'].strftime('%Y-%m-%d') if t['started_at'] else ''
        if day: s['daily'][day] += 1

    def avg(lst): return round(sum(lst)/len(lst), 1) if lst else 0

    result = []
    for nome, s in stats.items():
        result.append({
            'nome': nome,
            'open': s['open'], 'closed': s['closed'], 'total': s['total'],
            'avg_ticket_time':    avg(s['ticket_times']),
            'avg_waiting_time':   avg(s['waiting_times']),
            'avg_messaging_time': avg(s['messaging_times']),
            'avg_transfers':      avg(s['transfers']),
            'daily': dict(sorted(s['daily'].items())),
        })

    max_total = max((c['total'] for c in result), default=1) or 1
    max_wait  = max((c['avg_waiting_time'] for c in result), default=1) or 1
    max_conv  = max((c['avg_messaging_time'] for c in result), default=1) or 1
    for c in result:
        c['score'] = round(
            (c['total']/max_total)*40 +
            ((max_wait-c['avg_waiting_time'])/max_wait if max_wait else 0)*35 +
            ((max_conv-c['avg_messaging_time'])/max_conv if max_conv else 0)*25, 1)
    result.sort(key=lambda x: -x['score'])
    for i, c in enumerate(result): c['rank'] = i+1

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
        'consultores': result, 'totais': totais,
        'daily': [{'date':k,'count':v} for k,v in sorted(daily_all.items())],
        'period': {'from': date_from, 'to': date_to},
    })

@app.route('/api/stats')
def api_stats():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT COUNT(*) as n FROM tickets')
    total = cur.fetchone()['n']
    cur.execute('SELECT COUNT(*) as n FROM webhook_log')
    logs = cur.fetchone()['n']
    cur.execute('SELECT MAX(updated_at) as t FROM tickets')
    last = cur.fetchone()['t']
    cur.close()
    conn.close()
    return jsonify({'total_tickets': total, 'total_webhooks': logs,
                    'last_update': str(last) if last else None})

@app.route('/api/webhook-log')
def webhook_log():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute('SELECT id,event_type,received_at FROM webhook_log ORDER BY received_at DESC LIMIT 100')
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(rows)

@app.route('/')
def index():
    with open(os.path.join(os.path.dirname(__file__), 'static', 'index.html'), 'r', encoding='utf-8') as f:
        return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
