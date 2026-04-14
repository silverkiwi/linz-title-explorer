-- Run these in order after bronze ingest has completed into main.bronze.*

CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.bronze;
CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.gold;
CREATE SCHEMA IF NOT EXISTS main.ops;

-- Next: run sql/02_create_silver_tables_main.sql
-- Then: run sql/03_create_gold_title_search_main.sql
