-- Safe Silver + Gold build (fully qualified, with dependency checks)
-- Catalog: main
-- PATCHED VERSION: optimized for 15M+ row title-instrument-title

USE CATALOG main;

CREATE SCHEMA IF NOT EXISTS main.bronze;
CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.gold;

-- ============================================================
-- 1) Check required BRONZE tables exist
-- ============================================================
CREATE OR REPLACE TEMP VIEW required_bronze_tables AS
SELECT * FROM VALUES
  ('linz_title_raw'),
  ('linz_title_estate_raw'),
  ('linz_title_instrument_raw'),
  ('linz_title_instrument_title_raw'),
  ('linz_title_hierarchy_raw'),
  ('linz_title_encumbrance_raw'),
  ('linz_encumbrance_raw'),
  ('linz_encumbrance_share_raw'),
  ('linz_title_document_reference_raw'),
  ('linz_transaction_type_raw')
AS t(table_name);

CREATE OR REPLACE TEMP VIEW missing_bronze_tables AS
SELECT r.table_name
FROM required_bronze_tables r
LEFT JOIN main.information_schema.tables i
  ON i.table_schema = 'bronze'
 AND i.table_name = r.table_name
WHERE i.table_name IS NULL;

-- Show any missing bronze dependencies
SELECT * FROM missing_bronze_tables;

-- Fail fast if missing
SELECT assert_true(
  COUNT(*) = 0,
  CONCAT('Missing required bronze tables in main.bronze: ', CONCAT_WS(', ', COLLECT_LIST(table_name)))
)
FROM missing_bronze_tables;

-- ============================================================
-- 2) Build SILVER tables with optimizations
-- ============================================================

-- Set shuffle partitions for large tables
SET spark.sql.shuffle.partitions = 2000;
SET spark.databricks.optimizer.adaptive.enabled = true;

CREATE OR REPLACE TABLE main.silver.linz_title
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT
  CAST(title_no AS STRING)               AS title_no,
  CAST(ldt_loc_id AS BIGINT)             AS ldt_loc_id,
  CAST(register_t AS STRING)             AS register_type,
  CAST(ste_id AS BIGINT)                 AS ste_id,
  -- FIX: Use to_timestamp for proper datetime parsing
  to_date(to_timestamp(issue_date, 'yyyy-MM-dd HH:mm:ss')) AS issue_date,
  CAST(guarantee_ AS STRING)             AS guarantee_status,
  CAST(status AS STRING)                 AS status,
  CAST(type AS STRING)                   AS type,
  CAST(provisiona AS STRING)             AS provisional,
  CAST(sur_wrk_id AS BIGINT)             AS sur_wrk_id,
  NULLIF(CAST(ttl_title_ AS STRING), '') AS ttl_title_no_srs,
  NULLIF(CAST(ttl_titl_1 AS STRING), '') AS ttl_title_no_head_srs,
  NULLIF(CAST(maori_land AS STRING), '') AS maori_land,
  CAST(audit_id AS BIGINT)               AS audit_id,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_raw;

OPTIMIZE main.silver.linz_title ZORDER BY (title_no);

CREATE OR REPLACE TABLE main.silver.linz_title_estate
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(ttl_title_ AS STRING)             AS ttl_title_no,
  CAST(type AS STRING)                   AS type,
  CAST(status AS STRING)                 AS status,
  CAST(lgd_id AS BIGINT)                 AS lgd_id,
  NULLIF(CAST(share AS STRING), '')      AS share,
  NULLIF(CAST(timeshare_ AS STRING), '') AS timeshare_week_no,
  NULLIF(CAST(purpose AS STRING), '')    AS purpose,
  CAST(act_tin_id AS BIGINT)             AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)             AS act_id_crt,
  NULLIF(CAST(original_f AS STRING), '') AS original_flag,
  NULLIF(CAST(term AS STRING), '')       AS term,
  CAST(tin_id_ori AS BIGINT)             AS tin_id_orig,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_estate_raw;

OPTIMIZE main.silver.linz_title_estate ZORDER BY (ttl_title_no);

CREATE OR REPLACE TABLE main.silver.linz_title_instrument
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(inst_no AS STRING)             AS inst_no,
  CAST(trt_grp AS STRING)             AS trt_grp,
  CAST(trt_type AS STRING)            AS trt_type,
  CAST(ldt_loc_id AS BIGINT)          AS ldt_loc_id,
  CAST(status AS STRING)              AS status,
  -- FIX: Proper timestamp parsing
  to_timestamp(lodged_dat, 'yyyy-MM-dd HH:mm:ss') AS lodged_datetime,
  CAST(dlg_id AS BIGINT)              AS dlg_id,
  CAST(priority_n AS BIGINT)          AS priority_no,
  CAST(tin_id_par AS BIGINT)          AS tin_id_parent,
  CAST(audit_id AS BIGINT)            AS audit_id,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_instrument_raw;

OPTIMIZE main.silver.linz_title_instrument ZORDER BY (id);

-- CRITICAL FIX: Keep lineage columns for 15M+ row table
CREATE OR REPLACE TABLE main.silver.linz_title_instrument_title
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.targetFileSize' = '128mb'
)
AS
SELECT
  CAST(tin_id AS BIGINT)      AS tin_id,
  CAST(ttl_title_ AS STRING)  AS ttl_title_no,
  CAST(audit_id AS BIGINT)    AS audit_id,
  source_file,
  source_table,
  source_extract_date,
  ingested_at,
  ingest_batch_id,
  run_id
FROM main.bronze.linz_title_instrument_title_raw;

-- ZORDER immediately for join performance
OPTIMIZE main.silver.linz_title_instrument_title ZORDER BY (ttl_title_no, tin_id);

CREATE OR REPLACE TABLE main.silver.linz_title_hierarchy
USING DELTA
AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(status AS STRING)                 AS status,
  NULLIF(CAST(ttl_title_ AS STRING), '') AS ttl_title_no_prior,
  NULLIF(CAST(ttl_titl_1 AS STRING), '') AS ttl_title_no_flw,
  CAST(tdr_id AS BIGINT)                 AS tdr_id,
  CAST(act_tin_id AS BIGINT)             AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)             AS act_id_crt,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_hierarchy_raw;

CREATE OR REPLACE TABLE main.silver.linz_encumbrance
USING DELTA
AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(status AS STRING)              AS status,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_tin__1 AS BIGINT)          AS act_tin_id_orig,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  CAST(act_id_ori AS BIGINT)          AS act_id_orig,
  NULLIF(CAST(term AS STRING), '')    AS term,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_encumbrance_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_encumbrance
USING DELTA
AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(ttl_title_ AS STRING)          AS ttl_title_no,
  CAST(enc_id AS BIGINT)              AS enc_id,
  CAST(status AS STRING)              AS status,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_encumbrance_raw;

OPTIMIZE main.silver.linz_title_encumbrance ZORDER BY (ttl_title_no);

CREATE OR REPLACE TABLE main.silver.linz_encumbrance_share
USING DELTA
AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(enc_id AS BIGINT)                 AS enc_id,
  CAST(status AS STRING)                 AS status,
  CAST(act_tin_id AS BIGINT)             AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)             AS act_id_crt,
  NULLIF(CAST(share AS STRING), '')      AS share,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_encumbrance_share_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_document_reference
USING DELTA
AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(tin_id AS BIGINT)                 AS tin_id,
  CAST(type AS STRING)                   AS type,
  NULLIF(CAST(reference AS STRING), '')  AS reference,
  source_extract_date,
  ingested_at
FROM main.bronze.linz_title_document_reference_raw;

CREATE OR REPLACE TABLE main.silver.linz_transaction_type
USING DELTA
AS
SELECT
  CAST(grp AS STRING)                    AS grp,
  CAST(type AS STRING)                   AS type,
  NULLIF(CAST(description AS STRING), '') AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.linz_transaction_type_raw;

-- ============================================================
-- 3) Build GOLD - SPLIT INTO CORE AND DETAIL
-- ============================================================

-- GOLD CORE: Lightweight, no arrays, for fast search
CREATE OR REPLACE TABLE main.gold.linz_title_search_core
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
WITH
estate_agg AS (
  SELECT
    ttl_title_no,
    COUNT(*) AS estate_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_estate_count,
    MAX(CASE WHEN is_current THEN type END) AS primary_estate_type
  FROM main.silver.linz_title_estate
  GROUP BY ttl_title_no
),
instrument_detail AS (
  SELECT
    tit.ttl_title_no,
    ti.id AS tin_id,
    ti.inst_no,
    ti.trt_type,
    tt.description AS transaction_type_description,
    ti.status,
    ti.lodged_datetime
  FROM main.silver.linz_title_instrument_title tit
  JOIN main.silver.linz_title_instrument ti
    ON tit.tin_id = ti.id
  LEFT JOIN main.silver.linz_transaction_type tt
    ON ti.trt_grp = tt.grp AND ti.trt_type = tt.type
),
instrument_agg AS (
  SELECT
    ttl_title_no,
    COUNT(DISTINCT tin_id) AS instrument_count,
    SUM(CASE WHEN status IN ('REGD', 'LIVE') THEN 1 ELSE 0 END) AS registered_instrument_count,
    max_by(tin_id, coalesce(lodged_datetime, TIMESTAMP '1900-01-01')) AS latest_instrument_id,
    max_by(inst_no, coalesce(lodged_datetime, TIMESTAMP '1900-01-01')) AS latest_instrument_no,
    max_by(trt_type, coalesce(lodged_datetime, TIMESTAMP '1900-01-01')) AS latest_instrument_type,
    max_by(transaction_type_description, coalesce(lodged_datetime, TIMESTAMP '1900-01-01')) AS latest_instrument_description,
    max(lodged_datetime) AS latest_lodged_date
  FROM instrument_detail
  GROUP BY ttl_title_no
),
encumbrance_agg AS (
  SELECT
    te.ttl_title_no,
    COUNT(*) AS encumbrance_count,
    SUM(CASE WHEN te.is_current THEN 1 ELSE 0 END) AS current_encumbrance_count,
    MAX(CASE WHEN te.is_current THEN TRUE ELSE FALSE END) AS has_current_encumbrance
  FROM main.silver.linz_title_encumbrance te
  GROUP BY te.ttl_title_no
),
hierarchy_agg AS (
  SELECT
    ttl_title_no_prior AS title_no,
    COUNT(*) AS follow_on_title_count,
    COUNT(DISTINCT ttl_title_no_flw) AS prior_title_count
  FROM main.silver.linz_title_hierarchy
  WHERE ttl_title_no_prior IS NOT NULL
  GROUP BY ttl_title_no_prior
)
SELECT
  t.title_no,
  t.status AS title_status,
  t.type AS title_type,
  t.register_type,
  t.issue_date,
  t.guarantee_status,
  t.maori_land,
  t.is_current,
  t.ste_id,
  t.ldt_loc_id,
  -- Aggregates only, no arrays
  coalesce(e.estate_count, 0) AS estate_count,
  coalesce(e.current_estate_count, 0) AS current_estate_count,
  e.primary_estate_type,
  coalesce(i.instrument_count, 0) AS instrument_count,
  coalesce(i.registered_instrument_count, 0) AS registered_instrument_count,
  i.latest_instrument_id,
  i.latest_instrument_no,
  i.latest_instrument_type,
  i.latest_instrument_description,
  i.latest_lodged_date,
  coalesce(enc.encumbrance_count, 0) AS encumbrance_count,
  coalesce(enc.current_encumbrance_count, 0) AS current_encumbrance_count,
  coalesce(enc.has_current_encumbrance, FALSE) AS has_current_encumbrance,
  coalesce(h.prior_title_count, 0) AS prior_title_count,
  coalesce(h.follow_on_title_count, 0) AS follow_on_title_count,
  -- FIX: lowercase for case-insensitive search
  lower(concat_ws(' ',
    t.title_no,
    t.type,
    t.register_type,
    coalesce(i.latest_instrument_no, ''),
    coalesce(i.latest_instrument_description, '')
  )) AS search_text,
  current_timestamp() AS updated_at
FROM main.silver.linz_title t
LEFT JOIN estate_agg e ON t.title_no = e.ttl_title_no
LEFT JOIN instrument_agg i ON t.title_no = i.ttl_title_no
LEFT JOIN encumbrance_agg enc ON t.title_no = enc.ttl_title_no
LEFT JOIN hierarchy_agg h ON t.title_no = h.title_no;

OPTIMIZE main.gold.linz_title_search_core
ZORDER BY (title_no, title_status, is_current);

-- GOLD DETAIL: Only for titles that need full arrays (optional, build separately)
-- Uncomment to build full detail table (warning: expensive with 15M+ links)
/*
CREATE OR REPLACE TABLE main.gold.linz_title_search_detail
AS
-- [Full query with collect_list arrays from original]
-- Recommend filtering to current titles only:
-- WHERE t.is_current = true
*/

-- Quick sanity check
SELECT 
  COUNT(*) AS gold_core_rows,
  SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_titles,
  MAX(updated_at) AS last_updated
FROM main.gold.linz_title_search_core;
