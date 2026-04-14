# LINZ title-search MVP

Files added:

- `linz-databricks-website-plan.md` — overall plan
- `scripts/databricks_ingest_linz_dbf.py` — chunked DBF -> Delta bronze ingester
- `sql/01_create_schemas.sql` — schema bootstrap
- `sql/02_create_silver_tables.sql` — silver normalized tables
- `sql/03_create_gold_title_search.sql` — wide title-centric serving table

## Current data shape

The current `lds-world-10layers-DBF.zip` package supports a **title-centric MVP**.

It does **not yet** contain the parcel/appellation/owner/map layers needed for the final map-first property UX.

## Suggested execution order in Databricks

1. Upload and unzip the package to DBFS / a Volume
2. Install `dbfread`
3. Run the ingest script to populate `bronze.*`
4. Run the SQL files in order
5. Build API + frontend against `gold.linz_title_search`

## Example Databricks notebook flow

```python
%pip install dbfread
```

```python
# Adjust paths/catalog as needed
%run ./scripts/databricks_ingest_linz_dbf.py \
  --source-dir /dbfs/FileStore/linz/lds-world-10layers-unzipped \
  --catalog main \
  --bronze-schema bronze \
  --extract-date 2026-04-06
```

Then execute:

- `sql/01_create_schemas.sql`
- `sql/02_create_silver_tables.sql`
- `sql/03_create_gold_title_search.sql`

## Next recommended step

After the Databricks tables are built, scaffold:

- `GET /api/search?q=`
- `GET /api/title/:titleNo`

and a simple frontend with:

- search box
- result list
- detail page
