# LINZ website (Flask)

Simple title-search website backed by Databricks `main.gold.linz_title_search`.

## 1) Setup

```bash
cd webapp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` and set your Databricks token.

## 2) Run

```bash
python app.py
```

Open: http://localhost:8080

## Endpoints

- `GET /api/health`
- `GET /api/search?q=...&limit=50&offset=0&sort_by=issue_date&sort_dir=DESC`
- `GET /api/search.csv?q=...&limit=5000&offset=0&sort_by=issue_date&sort_dir=DESC`
- `GET /api/instruments/search?q=...&limit=100`
- `GET /api/instruments/search.csv?q=...&limit=5000`
- `GET /api/instrument/<inst_no>`
- `GET /api/title/<title_no>`
- `GET /api/title/<title_no>/instruments`
- `GET /api/title/<title_no>/instruments.csv`
- `GET /api/title/<title_no>/encumbrances`
- `GET /api/title/<title_no>/encumbrances.csv`
- `GET /api/title/<title_no>/lineage`
- `GET /api/title/<title_no>/lineage/graph`

## Notes

- Uses Databricks SQL connector
- Reads from table configured by env vars (defaults to `main.gold.linz_title_search`)
- For production, run behind gunicorn and rotate token regularly
