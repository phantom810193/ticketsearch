# Ticketsearch Backend

This service exposes LIFF-compatible APIs and helper utilities for monitoring ibon tickets.

## Running locally

```bash
pip install -r requirements.txt
gunicorn -w 1 -b 0.0.0.0:8000 app:app
```

## Route compatibility strategy

* New LIFF APIs live under `/api/liff/*` and power the LIFF client.
* Legacy endpoints `/liff/activities`, `/liff/watch`, and `/liff/unwatch` are mapped directly to the new handlers so existing deployments keep working; these aliases will stay for at least two release cycles (or until all clients migrate).

## Deployment smoke check

After deploying to Cloud Run you can verify the key endpoints stay healthy:

```bash
BASE="$(gcloud run services describe ticketsearch --region=asia-east1 --format='value(status.url)')"
bash scripts/smoke_check.sh "$BASE"
```
