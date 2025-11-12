import http.server
import socketserver
import json
import threading
import time
import uuid
import os
import csv
import zipfile
from urllib.parse import urlparse, parse_qs

EXPORT_DIR = os.path.join(os.path.dirname(__file__), 'exports')
ALLOWED_ORIGIN = '*'
PORT = 5512

jobs = {}
jobs_lock = threading.Lock()

class ExportJob(threading.Thread):
    def __init__(self, job_id, job_type, payload):
        super().__init__(daemon=True)
        self.job_id = job_id
        self.job_type = job_type
        self.payload = payload or {}

    def update(self, **kwargs):
        with jobs_lock:
            jobs[self.job_id].update(kwargs)

    def run(self):
        try:
            start = time.time()
            self.update(status='running', percent=2, message='Preparazione export…')
            os.makedirs(EXPORT_DIR, exist_ok=True)

            # Decide output file names
            base_name = f"{self.job_type}_{time.strftime('%Y%m%d_%H%M%S')}"
            csv_path = os.path.join(EXPORT_DIR, base_name + '.csv')
            zip_path = os.path.join(EXPORT_DIR, base_name + '.zip')

            # Select dataset and headers
            rows, headers = self._collect_rows()
            total = max(1, len(rows))

            # Write CSV incrementally to avoid memory spikes
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for i, row in enumerate(rows, start=1):
                    writer.writerow(row)
                    if i % 200 == 0 or i == total:
                        pct = round(i * 100 / total)
                        self.update(percent=min(pct, 99), message=f'Esportazione righe {i}/{total}…')
                        time.sleep(0)  # yield

            # Small pause before compression stage for clearer feedback
            try:
                self.update(percent=99, message='Compressione risultati…')
            except Exception:
                pass

            # Compress to ZIP
            with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(csv_path, arcname=os.path.basename(csv_path))

            # Cleanup CSV (keep only ZIP)
            try:
                os.remove(csv_path)
            except Exception:
                pass

            elapsed = int(time.time() - start)
            self.update(status='done', percent=100, message=f'Export completato in {elapsed}s', file_path=zip_path)
        except Exception as e:
            self.update(status='error', message=f'Errore export: {e}')

    def _collect_rows(self):
        jt = (self.job_type or '').upper()
        data = self.payload or {}
        if jt == 'LGA':
            headers = ['File Name', 'Data', 'Ora', 'Type', 'Sev', 'Oggetto', 'Descrizione', 'Dettaglio']
            items = data.get('lga', [])
            rows = [[
                it.get('fileName',''), it.get('dateIso',''), it.get('time',''), it.get('type',''),
                it.get('severity',''), it.get('object',''), it.get('title',''), it.get('detail','')
            ] for it in items]
            return rows, headers
        if jt == 'LGE':
            headers = ['File Name', 'Data', 'Ora', 'Type', 'Sev', 'Oggetto', 'Descrizione', 'Dettaglio']
            items = data.get('lge', [])
            rows = [[
                it.get('fileName',''), it.get('dateIso',''), it.get('time',''), it.get('type',''),
                it.get('severity',''), it.get('object',''), it.get('title',''), it.get('detail','')
            ] for it in items]
            return rows, headers
        if jt == 'LGD':
            headers = ['File Name', 'Metric', 'NodeUpgrade', 'NodeManual', 'NodeSpontaneous', 'AllNodeRestarts', 'PartialOutages']
            items = data.get('lgd', [])
            rows = [[
                it.get('fileName',''), it.get('metric',''), it.get('nodeUpgrade',''), it.get('nodeManual',''),
                it.get('nodeSpontaneous',''), it.get('allNodeRestarts',''), it.get('partialOutages','')
            ] for it in items]
            return rows, headers
        if jt == 'LGD_RESTARTS':
            items = data.get('lgdRestarts', [])
            has_new = any('typeReason' in it for it in items)
            if has_new:
                headers = ['File Name', 'Data', 'Ora', 'Tipo/Ragione', 'Valore', 'Commento', 'Durata']
                rows = [[
                    it.get('fileName',''), it.get('dateIso',''), it.get('time',''), it.get('typeReason',''),
                    it.get('value',''), it.get('comment',''), it.get('duration','')
                ] for it in items]
            else:
                headers = ['File Name', 'Timestamp (UTC)', 'RestartType/Reason', 'SwVersion', 'SwRelease', 'RCS Downtime', 'Appl. Downtime', 'TN Downtime', 'RATs Downtime']
                rows = [[
                    it.get('fileName',''), it.get('timestamp',''), it.get('restartTypeReason',''), it.get('swVersion',''),
                    it.get('swRelease',''), it.get('rcsDowntime',''), it.get('applDowntime',''), it.get('tnDowntime',''), it.get('ratsDowntime','')
                ] for it in items]
            return rows, headers
        if jt == 'OUTAGES_COUNT':
            headers = ['PartialOutages Value', 'Count', 'File Names']
            outages = [it for it in data.get('lgd', []) if it.get('metric') == 'Number Of outages']
            count = {}
            for it in outages:
                key = it.get('partialOutages','')
                count[key] = count.get(key, 0) + 1
            rows = []
            for value, cnt in count.items():
                files = [it.get('fileName','') for it in outages if it.get('partialOutages','') == value]
                rows.append([value, cnt, ', '.join(files)])
            return rows, headers
        if jt == 'DOWNTIME_COUNT':
            headers = ['PartialOutages Downtime', 'Count', 'File Names']
            downtime = [it for it in data.get('lgd', []) if it.get('metric') == 'Total downtime']
            count = {}
            for it in downtime:
                key = it.get('partialOutages','')
                count[key] = count.get(key, 0) + 1
            rows = []
            for value, cnt in count.items():
                files = [it.get('fileName','') for it in downtime if it.get('partialOutages','') == value]
                rows.append([value, cnt, ', '.join(files)])
            return rows, headers
        # Default empty
        return [], ['No data']

class Handler(http.server.BaseHTTPRequestHandler):
    server_version = 'ExportServer/1.0'

    def _set_headers(self, code=200, content_type='application/json', extra_headers=None):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Access-Control-Allow-Origin', ALLOWED_ORIGIN)
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(200)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == '/export/start':
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length) if length > 0 else b''
            try:
                data = json.loads(body.decode('utf-8'))
            except Exception:
                data = {}
            job_type = (data.get('type') or '').upper()
            payload = data.get('data') or {}
            job_id = str(uuid.uuid4())
            with jobs_lock:
                jobs[job_id] = {
                    'status': 'queued',
                    'percent': 0,
                    'message': 'In coda...',
                    'file_path': None,
                    'started_at': time.time()
                }
            ExportJob(job_id, job_type, payload).start()
            self._set_headers(200)
            self.wfile.write(json.dumps({'job_id': job_id}).encode('utf-8'))
            return
        self._set_headers(404)
        self.wfile.write(json.dumps({'error': 'Not Found'}).encode('utf-8'))

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/export/status':
            qs = parse_qs(parsed.query)
            job_id = (qs.get('id') or [''])[0]
            with jobs_lock:
                info = jobs.get(job_id)
            if not info:
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'Invalid job'}).encode('utf-8'))
                return
            self._set_headers(200)
            self.wfile.write(json.dumps(info).encode('utf-8'))
            return
        if parsed.path == '/export/download':
            qs = parse_qs(parsed.query)
            job_id = (qs.get('id') or [''])[0]
            with jobs_lock:
                info = jobs.get(job_id)
            if not info or info.get('status') != 'done' or not info.get('file_path') or not os.path.isfile(info['file_path']):
                self._set_headers(404)
                self.wfile.write(json.dumps({'error': 'File non pronto'}).encode('utf-8'))
                return
            fp = info['file_path']
            fn = os.path.basename(fp)
            try:
                fs = os.path.getsize(fp)
            except Exception:
                fs = None
            headers = {
                'Content-Disposition': f'attachment; filename="{fn}"',
                'Cache-Control': 'no-cache',
            }
            if fs is not None:
                headers['Content-Length'] = str(fs)
            self._set_headers(200, content_type='application/zip', extra_headers=headers)
            with open(fp, 'rb') as f:
                while True:
                    chunk = f.read(1024 * 64)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            return
        self._set_headers(404)
        self.wfile.write(json.dumps({'error': 'Not Found'}).encode('utf-8'))

if __name__ == '__main__':
    import sys
    if len(sys.argv) >= 2:
        PORT = int(sys.argv[1])
    with socketserver.ThreadingTCPServer(('', PORT), Handler) as httpd:
        print(f'Export server running on http://127.0.0.1:{PORT}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            httpd.server_close()