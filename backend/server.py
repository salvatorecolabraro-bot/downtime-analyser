import os
import re
import io
import csv
import json
import uuid
import time
import zipfile
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DW_DIR = os.path.join(PROJECT_ROOT, 'DW')
EXPORT_DIR = os.path.join(PROJECT_ROOT, 'backend', 'exports')
USERS_FILE = os.path.join(PROJECT_ROOT, 'backend', 'users.json')

# In-memory job store
JOBS = {}


def ensure_dirs():
    try:
        os.makedirs(EXPORT_DIR, exist_ok=True)
    except Exception:
        pass


def load_users():
    try:
        if not os.path.isfile(USERS_FILE):
            return []
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def save_users(users):
    try:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def iter_log_files():
    if not os.path.isdir(DW_DIR):
        return []
    return [os.path.join(DW_DIR, n) for n in os.listdir(DW_DIR) if n.lower().endswith('.log')]


def count_stats():
    total_files = 0
    lga_count = 0
    lge_count = 0
    lgd_count = 0
    lgd_restarts_count = 0
    if not os.path.isdir(DW_DIR):
        return {
            'totalFiles': 0,
            'lgaCount': 0,
            'lgeCount': 0,
            'lgdCount': 0,
            'lgdRestartsCount': 0,
        }
    # Usa il parser per LGA/LGE e LGD restarts
    parsed = parse_logs_summary()
    lga_count = len(parsed.get('lga', []))
    lge_count = len(parsed.get('lge', []))
    lgd_restarts_count = len(parsed.get('lgdRestarts', []))
    # Conta le righe LGD statistiche come fa il frontend (metriche)
    for path in iter_log_files():
        total_files += 1
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    if (line.startswith('Number Of outages') or
                        line.startswith('Total downtime') or
                        line.startswith('Downtime per day') or
                        line.startswith('Downtime per outage')):
                        lgd_count += 1
        except Exception:
            pass
    return {
        'totalFiles': total_files,
        'lgaCount': lga_count,
        'lgeCount': lge_count,
        'lgdCount': lgd_count,
        'lgdRestartsCount': lgd_restarts_count,
    }


# Semplice parser per estrarre voci LGA/LGE e LGD restarts
SEMICOLON_ROW = re.compile(r"^(\d{4}-\d{2}-\d{2});(\d{2}:\d{2}:\d{2});(.*)$")
OLD_ROW = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(AL|EV)\s+([*mMw])\s+(.+)$")
DURATION_RX = re.compile(r"^(\d{1,2}):(\d{2}):(\d{2})$")


def parse_logs_summary():
    lga = []
    lge = []
    lgd_restarts = []
    for path in iter_log_files():
        fname = os.path.basename(path)
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith('='):
                        continue

                    # Gestione righe delimitate da punto e virgola
                    if ';' in line:
                        parts = [p.strip() for p in line.split(';')]
                        # Caso 1: data;ora;...
                        if len(parts) >= 3 and re.match(r'^\d{4}-\d{2}-\d{2}$', parts[0]) and re.match(r'^\d{2}:\d{2}:\d{2}$', parts[1]):
                            type_field = (parts[2] or '').strip().upper()
                            # AL/EV (LGA/LGE)
                            if type_field in ('AL', 'EV'):
                                item = {
                                    'fileName': fname,
                                    'dateIso': parts[0],
                                    'time': parts[1],
                                    'type': type_field,
                                    'severity': parts[3] if len(parts) > 3 else '',
                                    'object': parts[4] if len(parts) > 4 else '',
                                    'title': parts[5] if len(parts) > 5 else '',
                                    'detail': parts[6] if len(parts) > 6 else ''
                                }
                                if type_field == 'AL':
                                    lga.append(item)
                                else:
                                    lge.append(item)
                                continue
                            # LGD/LGDC eventi: non richiedere durata in formato HH:MM:SS (coerente con frontend)
                            if len(parts) >= 6:
                                ev = {
                                    'fileName': fname,
                                    'dateIso': parts[0],
                                    'time': parts[1],
                                    'typeReason': parts[2] or '',
                                    'value': parts[3] or '',
                                    'comment': parts[4] or '',
                                    'duration': parts[5] or ''
                                }
                                lgd_restarts.append(ev)
                                continue
                        # Caso 2: timestamp combinato "YYYY-MM-DD HH:MM:SS;..."
                        if len(parts) >= 2 and re.match(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$', parts[0]):
                            dateIso, time_ = parts[0][:10], parts[0][11:]
                            # Formato restart atteso: ts;Type/Reason;Value;Comment;Duration
                            if len(parts) >= 5:
                                ev = {
                                    'fileName': fname,
                                    'dateIso': dateIso,
                                    'time': time_,
                                    'typeReason': parts[1] or '',
                                    'value': parts[2] or '',
                                    'comment': parts[3] or '',
                                    'duration': parts[4] or ''
                                }
                                lgd_restarts.append(ev)
                                continue

                    # Fallback: vecchio formato spazio-delimitato per LGA/LGE
                    m2 = OLD_ROW.match(line)
                    if m2:
                        item = {
                            'fileName': fname,
                            'dateIso': m2.group(1),
                            'time': m2.group(2),
                            'type': m2.group(3),
                            'severity': m2.group(4),
                            'object': '',
                            'title': m2.group(5).strip(),
                            'detail': ''
                        }
                        if item['type'] == 'AL':
                            lga.append(item)
                        else:
                            lge.append(item)
                        continue
        except Exception:
            # ignora file non leggibili
            pass
    return {'lga': lga, 'lge': lge, 'lgdRestarts': lgd_restarts}


def top_counts(items, key, top_n=5):
    counter = {}
    for it in items:
        k = (it.get(key) or '').strip()
        if not k:
            k = 'N/D'
        counter[k] = counter.get(k, 0) + 1
    pairs = sorted(counter.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    return {'labels': [p[0] for p in pairs], 'data': [p[1] for p in pairs]}


def compute_charts_summary(top_n=5):
    data = parse_logs_summary()
    lga = data['lga']
    lge = data['lge']
    lgd = data['lgdRestarts']
    # Top LGA by Title
    lga_top_title = top_counts(lga, 'title', top_n)
    # LGE Top by Title
    lge_top_title = top_counts(lge, 'title', top_n)
    # LGA severity distribution (keep natural order by count desc)
    sev = {}
    for it in lga:
        s = (it.get('severity') or '').strip().upper() or 'N/D'
        sev[s] = sev.get(s, 0) + 1
    sev_pairs = sorted(sev.items(), key=lambda kv: kv[1], reverse=True)
    lga_severity = {'labels': [p[0] for p in sev_pairs], 'data': [p[1] for p in sev_pairs]}
    # LGD Top by TypeReason and by FileName
    lgd_top_type = top_counts(lgd, 'typeReason', top_n)
    lgd_top_node = top_counts(lgd, 'fileName', top_n)
    # LGD total duration by TypeReason
    def parse_duration_sec(s):
        """Interpreta durate come secondi da vari formati.
        Supporta:
        - HH:MM:SS
        - "123s" o "123 s"
        - combinazioni con unità: "20m29s", "1h 2m 3s"
        - forme estese miste: "1229s (20m29s)" (prende i secondi iniziali)
        """
        try:
            raw = (s or '').strip().lower()
            if not raw:
                return 0
            m = DURATION_RX.match(raw)
            if m:
                h = int(m.group(1) or '0')
                mi = int(m.group(2) or '0')
                se = int(m.group(3) or '0')
                return h * 3600 + mi * 60 + se
            # Preferisci secondi espliciti all'inizio (per evitare doppio conteggio di forme tra parentesi)
            m2 = re.match(r"^(\d+)\s*s\b", raw)
            if m2:
                return int(m2.group(1))
            # Altrimenti aggrega unità presenti ovunque nella stringa
            total = 0
            mh = re.search(r"(\d+)\s*h", raw)
            mm = re.search(r"(\d+)\s*m", raw)
            ms = re.search(r"(\d+)\s*s", raw)
            if mh:
                total += int(mh.group(1)) * 3600
            if mm:
                total += int(mm.group(1)) * 60
            if ms:
                total += int(ms.group(1))
            return total
        except Exception:
            return 0
    dmap = {}
    for it in lgd:
        k = (it.get('typeReason') or '').strip() or 'N/D'
        dmap[k] = dmap.get(k, 0) + parse_duration_sec(it.get('duration') or '')
    d_pairs = sorted(dmap.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    lgd_duration_by_type = {'labels': [p[0] for p in d_pairs], 'data': [p[1] for p in d_pairs]}
    return {
        'lgaTopByTitle': lga_top_title,
        'lgeTopByTitle': lge_top_title,
        'lgaSeverity': lga_severity,
        'lgdTopByTypeReason': lgd_top_type,
        'lgdTopByFileName': lgd_top_node,
        'lgdDurationByTypeReason': lgd_duration_by_type,
    }


def parse_lgd_metrics():
    """Costruisce le metriche LGD (Number Of outages, Total downtime, ecc.) dal contenuto dei file DW.
    Supporta righe delimitate da ';' e righe con colonne separate da spazi multipli.
    """
    items = []
    if not os.path.isdir(DW_DIR):
        return items
    for path in iter_log_files():
        fname = os.path.basename(path)
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    if (line.startswith('Number Of outages') or
                        line.startswith('Total downtime') or
                        line.startswith('Downtime per day') or
                        line.startswith('Downtime per outage')):
                        if ';' in line:
                            parts = [p.strip() for p in line.split(';')]
                            if len(parts) >= 6:
                                items.append({
                                    'fileName': fname,
                                    'metric': parts[0],
                                    'nodeUpgrade': parts[1],
                                    'nodeManual': parts[2],
                                    'nodeSpontaneous': parts[3],
                                    'allNodeRestarts': parts[4],
                                    'partialOutages': parts[5],
                                })
                        else:
                            parts = [p.strip() for p in re.split(r"\s{2,}", line)]
                            if len(parts) >= 6:
                                items.append({
                                    'fileName': fname,
                                    'metric': parts[0],
                                    'nodeUpgrade': parts[1],
                                    'nodeManual': parts[2],
                                    'nodeSpontaneous': parts[3],
                                    'allNodeRestarts': parts[4],
                                    'partialOutages': parts[5],
                                })
        except Exception:
            pass
    return items


def build_csv(job_type, payload):
    jt = (job_type or '').upper()
    # Costruiamo CSV in memoria usando i campi come nel client
    output = io.StringIO()
    writer = csv.writer(output)

    def write_rows(headers, rows):
        if headers:
            writer.writerow(headers)
        for r in rows:
            writer.writerow([x if x is not None else '' for x in r])

    if jt in ('LGA', 'LGE'):
        headers = ['File Name', 'Data', 'Ora', 'Type', 'Sev', 'Oggetto', 'Descrizione', 'Dettaglio']
        key = 'lga' if jt == 'LGA' else 'lge'
        items = (payload.get(key) if isinstance(payload, dict) else None) or []
        if not items:
            # fallback: costruisci dal DW
            parsed = parse_logs_summary()
            items = parsed[key]
        rows = [
            [it.get('fileName',''), it.get('dateIso',''), it.get('time',''), it.get('type',''), it.get('severity',''), it.get('object',''), it.get('title',''), it.get('detail','')]
            for it in items
        ]
        write_rows(headers, rows)
        base = jt
    elif jt == 'LGD':
        headers = ['File Name','Metric','NodeUpgrade','NodeManual','NodeSpontaneous','AllNodeRestarts','PartialOutages']
        items = (payload.get('lgd') if isinstance(payload, dict) else None) or []
        write_rows(headers, [
            [it.get('fileName',''), it.get('metric',''), it.get('nodeUpgrade',''), it.get('nodeManual',''), it.get('nodeSpontaneous',''), it.get('allNodeRestarts',''), it.get('partialOutages','')]
            for it in items
        ])
        base = jt
    elif jt == 'LGD_RESTARTS':
        items = (payload.get('lgdRestarts') if isinstance(payload, dict) else None) or []
        has_new = any(('typeReason' in it) for it in items)
        # fallback dal DW se nessun dato inviato
        if not items:
            parsed = parse_logs_summary()
            items = parsed['lgdRestarts']
            has_new = True
        if has_new:
            headers = ['File Name','Data','Ora','Tipo/Ragione','Valore','Commento','Durata']
            rows = [[it.get('fileName',''), it.get('dateIso',''), it.get('time',''), it.get('typeReason',''), it.get('value',''), it.get('comment',''), it.get('duration','')] for it in items]
        else:
            headers = ['File Name','Timestamp (UTC)','RestartType/Reason','SwVersion','SwRelease','RCS Downtime','Appl. Downtime','TN Downtime','RATs Downtime']
            rows = [[it.get('fileName',''), it.get('timestamp',''), it.get('restartTypeReason',''), it.get('swVersion',''), it.get('swRelease',''), it.get('rcsDowntime',''), it.get('applDowntime',''), it.get('tnDowntime',''), it.get('ratsDowntime','')] for it in items]
        write_rows(headers, rows)
        base = jt
    elif jt in ('OUTAGES_COUNT', 'DOWNTIME_COUNT'):
        is_out = jt == 'OUTAGES_COUNT'
        headers = ['PartialOutages Value' if is_out else 'PartialOutages Downtime','Count','File Names']
        items = (payload.get('lgd') if isinstance(payload, dict) else None) or []
        # Se non arriva payload, non abbiamo una fonte affidabile per questi dati -> esportiamo intestazioni vuote
        write_rows(headers, [])
        base = jt
    else:
        write_rows(['No data'], [])
        base = 'DATA'

    return output.getvalue(), base


class APIHandler(BaseHTTPRequestHandler):
    def _set_headers(self, status=200, content_type='application/json', extra_headers=None):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()

    def log_message(self, fmt, *args):
        # meno rumoroso
        try:
            return super().log_message(fmt, *args)
        except Exception:
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query or '')
        # Root handler: serve index.html
        if path == '/' or path == '/index.html':
            try:
                abs_path = os.path.join(PROJECT_ROOT, 'index.html')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, 'text/html; charset=utf-8', extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
            except Exception:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                return
        # Admin handler: serve admin.html
        if path == '/admin' or path == '/admin.html':
            try:
                abs_path = os.path.join(PROJECT_ROOT, 'admin.html')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, 'text/html; charset=utf-8', extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
            except Exception:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                return
        # Stats handler: serve stats.html
        if path == '/stats' or path == '/stats.html':
            try:
                abs_path = os.path.join(PROJECT_ROOT, 'stats.html')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, 'text/html; charset=utf-8', extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
            except Exception:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                return
        # Event Detail handler: serve event_detail.html
        if path == '/event_detail' or path == '/event_detail.html':
            try:
                abs_path = os.path.join(PROJECT_ROOT, 'event_detail.html')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, 'text/html; charset=utf-8', extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
            except Exception:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                return
        # Alarm Detail handler: serve alarm_detail.html
        if path == '/alarm_detail' or path == '/alarm_detail.html':
            try:
                abs_path = os.path.join(PROJECT_ROOT, 'alarm_detail.html')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, 'text/html; charset=utf-8', extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
            except Exception:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                return
        # LGD Detail handler: serve lgd_detail.html
        if path == '/lgd_detail' or path == '/lgd_detail.html':
            try:
                abs_path = os.path.join(PROJECT_ROOT, 'lgd_detail.html')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, 'text/html; charset=utf-8', extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
            except Exception:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                return
        # Node Detail handler: serve node_detail.html
        if path == '/node_detail' or path == '/node_detail.html':
            try:
                abs_path = os.path.join(PROJECT_ROOT, 'node_detail.html')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, 'text/html; charset=utf-8', extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
            except Exception:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                return
        if path == '/api/ping':
            self._set_headers(200)
            self.wfile.write(json.dumps({'ok': True}).encode('utf-8'))
            return
        if path == '/api/admin/users':
            users = load_users()
            self._set_headers(200)
            self.wfile.write(json.dumps({'users': users}).encode('utf-8'))
            return
        if path == '/api/stats/header':
            stats = count_stats()
            self._set_headers(200)
            self.wfile.write(json.dumps(stats).encode('utf-8'))
            return
        if path == '/api/charts/summary':
            try:
                top = int(qs.get('n', ['5'])[0])
            except Exception:
                top = 5
            charts = compute_charts_summary(max(1, min(20, top)))
            self._set_headers(200)
            self.wfile.write(json.dumps(charts).encode('utf-8'))
            return
        # Nuovo: dettagli LGA/LGE con filtri (per abilitare drilldown severità in backend)
        if path == '/api/lga' or path == '/api/lge':
            try:
                parsed = parse_logs_summary()
                key = 'lga' if path.endswith('/lga') else 'lge'
                items = parsed.get(key, [])
                severity = (qs.get('severity', [''])[0] or '').strip().upper()
                node = (qs.get('node', [''])[0] or '').strip()
                date_from = (qs.get('from', [''])[0] or '').strip()
                date_to = (qs.get('to', [''])[0] or '').strip()
                try:
                    limit = int((qs.get('limit', ['200'])[0] or '200'))
                except Exception:
                    limit = 200
                node_base = os.path.splitext(node)[0] if node else ''
                node_log = (node_base + '.log') if node_base else ''
                out = []
                for it in items:
                    if severity:
                        s = (it.get('severity') or '').strip().upper()
                        if s != severity:
                            continue
                    if node_base:
                        fname = (it.get('fileName') or '').strip()
                        if fname not in (node_base, node_log):
                            continue
                    di = (it.get('dateIso') or it.get('date') or '').strip()
                    if date_from and di and di < date_from:
                        continue
                    if date_to and di and di > date_to:
                        continue
                    out.append(it)
                    if len(out) >= max(1, min(10000, limit)):
                        break
                self._set_headers(200)
                self.wfile.write(json.dumps({key: out}).encode('utf-8'))
                return
            except Exception as e:
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Errore interno', 'detail': str(e)}).encode('utf-8'))
                return
        # Nuovo: dettagli LGD restarts filtrati per typeReason, nodo, data
        if path == '/api/lgd':
            try:
                parsed = parse_logs_summary()
                items = parsed.get('lgdRestarts', [])
                # Normalizza typeReason: senza spazi, case-insensitive
                tr_q = (qs.get('typeReason', [''])[0] or '').strip()
                norm_tr = re.sub(r"\s+", '', tr_q).lower()
                node = (qs.get('node', [''])[0] or '').strip()
                date_from = (qs.get('from', [''])[0] or '').strip()
                date_to = (qs.get('to', [''])[0] or '').strip()
                try:
                    limit = int((qs.get('limit', ['1000'])[0] or '1000'))
                except Exception:
                    limit = 1000
                node_base = os.path.splitext(node)[0] if node else ''
                node_log = (node_base + '.log') if node_base else ''
                out = []
                for it in items:
                    # Type/Reason match
                    ev_tr = (it.get('typeReason') or it.get('restartTypeReason') or '').strip()
                    if norm_tr:
                        if re.sub(r"\s+", '', ev_tr).lower() != norm_tr:
                            continue
                    # Node match (se richiesto)
                    if node_base:
                        fname = (it.get('fileName') or '').strip()
                        if fname not in (node_base, node_log):
                            continue
                    # Date range
                    di = (it.get('dateIso') or it.get('date') or '').strip()
                    if date_from and di and di < date_from:
                        continue
                    if date_to and di and di > date_to:
                        continue
                    out.append(it)
                    if len(out) >= max(1, min(10000, limit)):
                        break
                # Ordina per data/ora decrescente
                def to_ms(it):
                    d = (it.get('dateIso') or it.get('date') or '')
                    t = (it.get('time') or '00:00:00')
                    try:
                        return int(time.mktime(time.strptime(f"{d} {t}", "%Y-%m-%d %H:%M:%S")))
                    except Exception:
                        return 0
                out.sort(key=to_ms, reverse=True)
                self._set_headers(200)
                self.wfile.write(json.dumps({'lgdRestarts': out}).encode('utf-8'))
                return
            except Exception as e:
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Errore interno', 'detail': str(e)}).encode('utf-8'))
                return
        # Metriche LGD
        if path == '/api/lgd_metrics':
            try:
                items = parse_lgd_metrics()
                node = (qs.get('node', [''])[0] or '').strip()
                metric_q = (qs.get('metric', [''])[0] or '').strip()
                try:
                    limit = int((qs.get('limit', ['2000'])[0] or '2000'))
                except Exception:
                    limit = 2000
                node_base = os.path.splitext(node)[0] if node else ''
                node_log = (node_base + '.log') if node_base else ''
                out = []
                for it in items:
                    if node_base:
                        fname = (it.get('fileName') or '').strip()
                        if fname not in (node_base, node_log):
                            continue
                    if metric_q:
                        m = (it.get('metric') or '').strip()
                        def norm(s):
                            return re.sub(r"\s+", ' ', (s or '').strip()).lower()
                        if norm(m) != norm(metric_q):
                            continue
                    out.append(it)
                    if len(out) >= max(1, min(10000, limit)):
                        break
                self._set_headers(200)
                self.wfile.write(json.dumps({'lgd': out}).encode('utf-8'))
                return
            except Exception as e:
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Errore interno', 'detail': str(e)}).encode('utf-8'))
                return
        if path == '/api/node/summary':
            # Ritorna gli eventi LGA, LGE e LGD_RESTARTS filtrati per nodo (fileName)
            node = (qs.get('node', [''])[0] or '').strip()
            if not node:
                self._set_headers(400)
                self.wfile.write(json.dumps({'error': 'Parametro "node" mancante'}).encode('utf-8'))
                return
            # Normalizza: accetta sia "CS0BE" che "CS0BE.log"
            node_base = os.path.splitext(node)[0]
            node_log = node_base + '.log'
            try:
                parsed = parse_logs_summary()
                def by_node(items):
                    out = []
                    for it in items or []:
                        fname = (it.get('fileName') or '').strip()
                        if not fname:
                            continue
                        if fname == node_base or fname == node_log:
                            out.append(it)
                    return out
                result = {
                    'fileName': node_log,
                    'lga': by_node(parsed.get('lga')), 
                    'lge': by_node(parsed.get('lge')), 
                    'lgdRestarts': by_node(parsed.get('lgdRestarts'))
                }
                self._set_headers(200)
                self.wfile.write(json.dumps(result).encode('utf-8'))
                return
            except Exception as e:
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Errore interno', 'detail': str(e)}).encode('utf-8'))
                return
        if path == '/api/files/list':
            # Ritorna elenco file nella cartella DW
            files = []
            try:
                if os.path.isdir(DW_DIR):
                    for name in os.listdir(DW_DIR):
                        ext = os.path.splitext(name)[1].lower()
                        if ext in ('.log', '.txt', '.csv'):
                            p = os.path.join(DW_DIR, name)
                            try:
                                st = os.stat(p)
                                files.append({
                                    'name': name,
                                    'size': st.st_size,
                                    'mtime': int(st.st_mtime)
                                })
                            except Exception:
                                files.append({'name': name})
            except Exception:
                files = []
            self._set_headers(200)
            self.wfile.write(json.dumps({'files': files}).encode('utf-8'))
            return
        if path == '/export/status':
            job_id = (qs.get('id') or [''])[0]
            info = JOBS.get(job_id)
            if not info:
                self._set_headers(404)
                self.wfile.write(json.dumps({'status': 'error', 'message': 'Job non trovato'}).encode('utf-8'))
                return
            self._set_headers(200)
            self.wfile.write(json.dumps({
                'status': info.get('status'),
                'percent': info.get('percent', 0),
                'message': info.get('message', '')
            }).encode('utf-8'))
            return
        if path == '/export/download':
            job_id = (qs.get('id') or [''])[0]
            info = JOBS.get(job_id)
            if not info or info.get('status') != 'done' or not os.path.isfile(info.get('zip_path','')):
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'File non pronto'}).encode('utf-8'))
                return
            zip_path = info['zip_path']
            with open(zip_path, 'rb') as fp:
                data = fp.read()
            self._set_headers(200, 'application/zip', extra_headers={
                'Content-Disposition': f'attachment; filename="{os.path.basename(zip_path)}"',
                'Content-Length': str(len(data)),
            })
            self.wfile.write(data)
            return
        # Static files serving: serve index and assets from project root
        try:
            # Map root to index.html
            rel = path.lstrip('/') or 'index.html'
            rel = os.path.normpath(rel)
            # Basic traversal protection
            if rel.startswith('..'):
                raise Exception('Traversal')
            # Disallow backend and DW direct exposure
            if rel.startswith('backend') or rel.startswith('DW'):
                raise Exception('Forbidden')
            # Allow only certain directories
            first = rel.split(os.sep)[0] if rel else ''
            allowed_dirs = ('', 'assets', 'exports')
            if first not in allowed_dirs:
                raise Exception('Forbidden')
            abs_path = os.path.join(PROJECT_ROOT, rel)
            if os.path.isfile(abs_path):
                ext = os.path.splitext(abs_path)[1].lower()
                content_types = {
                    '.html': 'text/html; charset=utf-8',
                    '.js': 'application/javascript; charset=utf-8',
                    '.css': 'text/css; charset=utf-8',
                    '.ico': 'image/x-icon',
                    '.png': 'image/png',
                    '.jpg': 'image/jpeg',
                    '.jpeg': 'image/jpeg',
                    '.svg': 'image/svg+xml',
                    '.zip': 'application/zip',
                }
                ct = content_types.get(ext, 'application/octet-stream')
                with open(abs_path, 'rb') as fp:
                    data = fp.read()
                self._set_headers(200, ct, extra_headers={'Content-Length': str(len(data))})
                self.wfile.write(data)
                return
        except Exception:
            pass
        # Not found
        self._set_headers(404)
        self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        ct_hdr = (self.headers.get('Content-Type') or '').lower()
        if 'application/json' in ct_hdr:
            try:
                length = int(self.headers.get('Content-Length', '0'))
                body = self.rfile.read(length) if length > 0 else b''
                payload = json.loads(body.decode('utf-8') or '{}')
            except Exception:
                payload = {}
        else:
            # Non consumare il body per consentire parsing multipart/form-data
            payload = {}
        if path == '/api/auth/login':
            # Login semplice: nessuna password, valida utente esistente
            users = load_users()
            username = (payload.get('username') or '').strip()
            email = (payload.get('email') or '').strip()
            found = None
            for u in users:
                if (username and (u.get('username') or '').strip() == username) or (email and (u.get('email') or '').strip() == email):
                    found = u
                    break
            if not found:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Credenziali non valide'}).encode('utf-8'))
                return
            self._set_headers(200)
            self.wfile.write(json.dumps({'ok': True, 'user': found}).encode('utf-8'))
            return
        if path == '/api/admin/users/create':
            users = load_users()
            username = (payload.get('username') or '').strip()
            email = (payload.get('email') or '').strip()
            role = (payload.get('role') or 'user').strip()
            active = bool(payload.get('active', True))
            password = (payload.get('password') or '').strip()
            if not username or not email:
                self._set_headers(400)
                self.wfile.write(json.dumps({'error': 'username ed email sono richiesti'}).encode('utf-8'))
                return
            if any((u.get('email') == email) or (u.get('username') == username) for u in users):
                self._set_headers(409)
                self.wfile.write(json.dumps({'error': 'Utente già esistente'}).encode('utf-8'))
                return
            uid = str(uuid.uuid4())
            now = int(time.time())
            user = {
                'id': uid,
                'username': username,
                'email': email,
                'role': role or 'user',
                'active': active,
                'password': password,
                'createdAt': now,
                'updatedAt': now,
            }
            users.append(user)
            save_users(users)
            self._set_headers(201)
            self.wfile.write(json.dumps({'ok': True, 'user': user}).encode('utf-8'))
            return
        if path == '/api/admin/users/update':
            users = load_users()
            uid = (payload.get('id') or '').strip()
            username_sel = (payload.get('username') or '').strip()
            email_sel = (payload.get('email') or '').strip()
            updated = None
            for u in users:
                if (uid and u.get('id') == uid) or (username_sel and (u.get('username') or '').strip() == username_sel) or (email_sel and (u.get('email') or '').strip() == email_sel):
                    # Aggiorna solo i campi presenti
                    for k in ['username', 'email', 'role', 'active', 'password']:
                        if k in payload:
                            u[k] = payload[k]
                    u['updatedAt'] = int(time.time())
                    updated = u
                    break
            if not updated:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Utente non trovato'}).encode('utf-8'))
                return
            save_users(users)
            self._set_headers(200)
            self.wfile.write(json.dumps({'ok': True, 'user': updated}).encode('utf-8'))
            return
        if path == '/api/admin/users/delete':
            users = load_users()
            uid = (payload.get('id') or '').strip()
            if not uid:
                self._set_headers(400)
                self.wfile.write(json.dumps({'error': 'id richiesto'}).encode('utf-8'))
                return
            new_users = [u for u in users if u.get('id') != uid]
            if len(new_users) == len(users):
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Utente non trovato'}).encode('utf-8'))
                return
            save_users(new_users)
            self._set_headers(200)
            self.wfile.write(json.dumps({'ok': True}).encode('utf-8'))
            return
        if path == '/api/files/upload':
            saved = []
            try:
                ct = (self.headers.get('Content-Type') or '').lower()
                # Supporta multipart/form-data e JSON con contenuto inline
                if ct.startswith('multipart/form-data'):
                    import cgi
                    fs = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                           environ={'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': self.headers.get('Content-Type', '')})
                    fields = []
                    try:
                        fields = fs.list or []
                    except Exception:
                        fields = []
                    for f in fields:
                        try:
                            filename = os.path.basename(getattr(f, 'filename', '') or '')
                            if not filename:
                                continue
                            ext = os.path.splitext(filename)[1].lower()
                            if ext not in ('.log', '.txt', '.csv'):
                                continue
                            data = f.file.read() if getattr(f, 'file', None) else b''
                            if not data:
                                continue
                            dst = os.path.join(DW_DIR, filename)
                            with open(dst, 'wb') as out:
                                out.write(data)
                            saved.append(filename)
                        except Exception:
                            continue
                else:
                    # JSON: { files: [ { filename, content, base64? } ] } oppure singolo { filename, content }
                    items = []
                    if isinstance(payload.get('files'), list):
                        items = payload['files']
                    elif payload.get('filename') and (payload.get('content') or payload.get('text')):
                        items = [payload]
                    import base64
                    for it in items:
                        filename = os.path.basename(str(it.get('filename') or '').strip())
                        if not filename:
                            continue
                        ext = os.path.splitext(filename)[1].lower()
                        if ext not in ('.log', '.txt', '.csv'):
                            # se senza estensione, fallback .log
                            if not ext:
                                filename = filename + '.log'
                            else:
                                continue
                        raw = it.get('content') or it.get('text') or ''
                        if isinstance(raw, str):
                            try:
                                data = base64.b64decode(raw) if it.get('base64') else raw.encode('utf-8')
                            except Exception:
                                data = raw.encode('utf-8')
                        else:
                            try:
                                data = bytes(raw)  # potrebbe essere già bytes/bytearray
                            except Exception:
                                data = b''
                        if not data:
                            continue
                        dst = os.path.join(DW_DIR, filename)
                        with open(dst, 'wb') as out:
                            out.write(data)
                        saved.append(filename)
                stats = count_stats()
                total = 0
                try:
                    total = len([n for n in os.listdir(DW_DIR) if n.lower().endswith(('.log', '.txt', '.csv'))])
                except Exception:
                    total = 0
                self._set_headers(200)
                self.wfile.write(json.dumps({'ok': True, 'saved': saved, 'savedCount': len(saved), 'totalFiles': total, 'stats': stats}).encode('utf-8'))
                return
            except Exception as e:
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Upload fallito', 'detail': str(e)}).encode('utf-8'))
                return
        if path == '/api/files/delete':
            # Elimina file selezionati dalla cartella DW
            try:
                items = payload.get('files')
                if not isinstance(items, list) or not items:
                    self._set_headers(400)
                    self.wfile.write(json.dumps({'error': 'files (array) richiesto'}).encode('utf-8'))
                    return
                deleted = []
                for name in items:
                    try:
                        # Protezione base: niente path traversal, solo estensioni consentite
                        fname = os.path.basename(str(name or '').strip())
                        if not fname:
                            continue
                        ext = os.path.splitext(fname)[1].lower()
                        if ext not in ('.log', '.txt', '.csv'):
                            continue
                        p = os.path.join(DW_DIR, fname)
                        if os.path.isfile(p) and os.path.dirname(p) == DW_DIR:
                            os.remove(p)
                            deleted.append(fname)
                    except Exception:
                        continue
                stats = count_stats()
                self._set_headers(200)
                self.wfile.write(json.dumps({'ok': True, 'deleted': deleted, 'deletedCount': len(deleted), 'stats': stats}).encode('utf-8'))
                return
            except Exception as e:
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Delete fallito', 'detail': str(e)}).encode('utf-8'))
                return
        if path == '/export/start':
            job_type = (payload.get('type') or '').upper()
            data = payload.get('data') or {}

            # Crea job immediatamente concluso (la UI ha un heartbeat di progressione)
            job_id = str(uuid.uuid4())
            try:
                csv_text, base = build_csv(job_type, data)
                # Scrivi zip su disco
                ensure_dirs()
                zip_name = f"{base}_{time.strftime('%Y%m%d_%H%M%S')}_{job_id[:8]}.zip"
                zip_path = os.path.join(EXPORT_DIR, zip_name)
                with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                    # nome interno CSV
                    csv_name = f"{base}.csv"
                    zf.writestr(csv_name, csv_text.encode('utf-8'))
                JOBS[job_id] = {
                    'status': 'done',
                    'percent': 100,
                    'message': 'Pronto',
                    'zip_path': zip_path,
                    'type': base
                }
                self._set_headers(200)
                self.wfile.write(json.dumps({'job_id': job_id}).encode('utf-8'))
                return
            except Exception as e:
                JOBS[job_id] = {'status': 'error', 'percent': 0, 'message': str(e)}
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Export fallito'}).encode('utf-8'))
                return
        # Not found
        self._set_headers(404)
        self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))


if __name__ == '__main__':
    ensure_dirs()
    port = int(os.environ.get('PORT', '9000'))
    server = HTTPServer(('0.0.0.0', port), APIHandler)
    print(f"Backend server running at http://localhost:{port}/")
    print(f"DW directory: {DW_DIR}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()