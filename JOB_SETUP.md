# Databricks Jobs: one table per task

Use `scripts/linz_ingest_one_table_job.py` with one task per source table.

## 1) Create job task template

- Task type: Python script (or notebook `%run` wrapper)
- Script path: `scripts/linz_ingest_one_table_job.py`
- Cluster: dedicated Job cluster (recommended)
- Add task parameters:
  - `source_table`
  - `source_dir=/Volumes/main/default/linz_data/lds-world-10layers-unzipped`
  - `extract_date=2026-04-06`
  - `catalog=main`
  - `bronze_schema=bronze`
  - `ops_schema=ops`
  - `overwrite=true`

Optional:
- `chunk_size` (override defaults)

## 2) Create one task per table

Order:
1. `landonline-transaction-type`
2. `landonline-title-document-reference`
3. `landonline-title`
4. `landonline-title-hierarchy`
5. `landonline-encumbrance`
6. `landonline-encumbrance-share`
7. `landonline-title-encumbrance`
8. `landonline-title-estate`
9. `landonline-title-instrument`
10. `landonline-title-instrument-title`

Set task dependencies so each task starts after the previous succeeds.

## 3) Retry policy

In job/task settings:
- Retries: 2–3
- Min retry interval: 5–10 min

Script already retries batch writes internally.

## 4) After all 10 tasks succeed

Run SQL:
- `sql/04_build_silver_gold_safe_main.sql`

## 5) Quick validation SQL

```sql
SELECT table_name, row_count, status, finished_at
FROM main.ops.linz_ingest_progress
ORDER BY finished_at DESC;
```
