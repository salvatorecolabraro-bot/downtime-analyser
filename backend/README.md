# Backend minimale (senza dipendenze)

Avvio:

```
python backend\server.py
```

API:
- `GET /api/ping` → `{ ok: true }`
- `GET /api/stats/header` → `{ totalFiles, lgaCount, lgeCount, lgdCount, lgdRestartsCount }`
 - `GET /api/charts/summary?n=5` → sommari per grafici (Top e distribuzioni)
 - `GET /api/lga|/api/lge?severity=&node=&from=&to=&limit=` → dettagli filtrati
 - `GET /api/lgd?typeReason=&node=&from=&to=&limit=` → restart LGD filtrati

Note:
- I conteggi sono basati su occorrenze testuali nei file `.log` sotto `DW/`.
- Cache: il backend mantiene una cache in memoria basata su uno snapshot della cartella `DW` (numero file e ultima modifica). Quando i file non cambiano, evita di ripetere il parsing dei log, riducendo drasticamente i tempi di risposta per `/api/lga`, `/api/lge`, `/api/lgd`, `/api/stats/header` e `/api/charts/summary`.
- Server threaded: avvio con `ThreadingHTTPServer` per gestire più richieste in parallelo.
- Il server usa solo librerie standard Python per evitare installazioni.
- Porta di default: `9000` (configurabile via env `PORT`).