# Backend minimale (senza dipendenze)

Avvio:

```
python backend\server.py
```

API:
- `GET /api/ping` → `{ ok: true }`
- `GET /api/stats/header` → `{ totalFiles, lgaCount, lgeCount, lgdCount, lgdRestartsCount }`

Note:
- I conteggi sono basati su occorrenze testuali nei file `.log` sotto `DW/`.
- Il server usa solo librerie standard Python per evitare installazioni.
- Porta di default: `9000` (configurabile via env `PORT`).