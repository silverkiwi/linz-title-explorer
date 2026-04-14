-- Databricks SQL notebook / query sequence

-- Run after bronze tables have been created by the Python ingest notebook.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;

-- Then run the contents of:
-- sql/02_create_silver_tables.sql
-- sql/03_create_gold_title_search.sql
