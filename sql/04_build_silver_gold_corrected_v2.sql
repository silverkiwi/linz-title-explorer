-- Safe Silver + Gold build (fully qualified, with dependency checks)
-- Catalog: main
-- CORRECTED VERSION 2: fixes column names, restores semantics, adds optimizations

USE CATALOG main;

CREATE SCHEMA IF NOT EXISTS main.bronze;
CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.gold;

-- Performance settings for large tables (15M+ rows)
SET spark.sql.shuffle.partitions = 2000;
SET spark.databricks.optimizer.adaptive.enabled = true;

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

SELECT * FROM missing_bronze_tables;

SELECT assert_true(
  COUNT(*) = 0,
  CONCAT('Missing required bronze tables in main.bronze: ', CONCAT_WS(', ', COLLECT_LIST(table_name)))
)
FROM missing_bronze_tables;

-- ============================================================
-- 2) Build SILVER tables
--    Note: bronze ingested as STRINGs; cast/parse in silver.
-- ============================================================
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
  -- FIX 3: Revert to simple TO_DATE (bronze has date-only strings)
  TO_DATE(issue_date)                    AS issue_date,
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
  -- FIX 3: Revert to TO_DATE for date-only strings
  TO_DATE(lodged_dat)                 AS lodged_datetime,
  CAST(dlg_id AS BIGINT)              AS dlg_id,
  CAST(priority_n AS BIGINT)          AS priority_no,
  CAST(tin_id_par AS BIGINT)          AS tin_id_parent,
  CAST(audit_id AS BIGINT)            AS audit_id,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_instrument_raw;

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

-- FIX 2: Restore correct encumbrance_share projection (no 'share' column)
CREATE OR REPLACE TABLE main.silver.linz_encumbrance_share
USING DELTA
AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(enc_id AS BIGINT)                 AS enc_id,
  CAST(status AS STRING)                 AS status,
  CAST(act_tin_id AS BIGINT)             AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)             AS act_id_crt,
  CAST(act_id_ext AS BIGINT)             AS act_id_ext,
  CAST(act_tin__1 AS BIGINT)             AS act_tin_id_ext,
  NULLIF(CAST(system_crt AS STRING), '') AS system_created,
  NULLIF(CAST(system_ext AS STRING), '') AS system_extinguished,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_encumbrance_share_raw;

-- FIX 1: Use reference_ (not reference)
CREATE OR REPLACE TABLE main.silver.linz_title_document_reference
USING DELTA
AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(tin_id AS BIGINT)                 AS tin_id,
  CAST(type AS STRING)                   AS type,
  NULLIF(CAST(reference_ AS STRING), '') AS reference,
  source_extract_date,
  ingested_at
FROM main.bronze.linz_title_document_reference_raw;

-- FIX 1: Use descriptio (not description)
CREATE OR REPLACE TABLE main.silver.linz_transaction_type
USING DELTA
AS
SELECT
  CAST(grp AS STRING)                    AS grp,
  CAST(type AS STRING)                   AS type,
  NULLIF(CAST(descriptio AS STRING), '') AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.linz_transaction_type_raw;

-- Optimize large silver tables (run after creation)
OPTIMIZE main.silver.linz_title ZORDER BY (title_no);
OPTIMIZE main.silver.linz_title_instrument_title ZORDER BY (ttl_title_no, tin_id);
OPTIMIZE main.silver.linz_title_instrument ZORDER BY (id);
OPTIMIZE main.silver.linz_title_encumbrance ZORDER BY (ttl_title_no);

-- ============================================================
-- 3) Check required SILVER tables exist before GOLD
-- ============================================================
CREATE OR REPLACE TEMP VIEW required_silver_tables AS
SELECT * FROM VALUES
  ('linz_title'),
  ('linz_title_estate'),
  ('linz_title_instrument'),
  ('linz_title_instrument_title'),
  ('linz_title_hierarchy'),
  ('linz_title_encumbrance'),
  ('linz_encumbrance'),
  ('linz_title_document_reference'),
  ('linz_transaction_type')
AS t(table_name);

CREATE OR REPLACE TEMP VIEW missing_silver_tables AS
SELECT r.table_name
FROM required_silver_tables r
LEFT JOIN main.information_schema.tables i
  ON i.table_schema = 'silver'
 AND i.table_name = r.table_name
WHERE i.table_name IS NULL;

SELECT * FROM missing_silver_tables;

SELECT assert_true(
  COUNT(*) = 0,
  CONCAT('Missing required silver tables in main.silver: ', CONCAT_WS(', ', COLLECT_LIST(table_name)))
)
FROM missing_silver_tables;

-- ============================================================
-- 4) Build GOLD - CORE (lightweight, no arrays)
-- ============================================================
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
    max_by(tin_id, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_id,
    max_by(inst_no, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_no,
    max_by(trt_type, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_type,
    max_by(transaction_type_description, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_description,
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
-- FIX 4: Restore correct prior/follow-on logic
follow_on_agg AS (
  SELECT
    ttl_title_no_prior AS title_no,
    COUNT(*) AS follow_on_title_count,
    sort_array(collect_set(ttl_title_no_flw)) AS follow_on_titles
  FROM main.silver.linz_title_hierarchy
  WHERE ttl_title_no_prior IS NOT NULL AND ttl_title_no_flw IS NOT NULL
  GROUP BY ttl_title_no_prior
),
prior_agg AS (
  SELECT
    ttl_title_no_flw AS title_no,
    COUNT(*) AS prior_title_count,
    sort_array(collect_set(ttl_title_no_prior)) AS prior_titles
  FROM main.silver.linz_title_hierarchy
  WHERE ttl_title_no_prior IS NOT NULL AND ttl_title_no_flw IS NOT NULL
  GROUP BY ttl_title_no_flw
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
  coalesce(p.prior_title_count, 0) AS prior_title_count,
  coalesce(f.follow_on_title_count, 0) AS follow_on_title_count,
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
LEFT JOIN prior_agg p ON t.title_no = p.title_no
LEFT JOIN follow_on_agg f ON t.title_no = f.title_no;

OPTIMIZE main.gold.linz_title_search_core
ZORDER BY (title_no, title_status, is_current);

-- FIX 6: Create compatibility view for downstream
CREATE OR REPLACE VIEW main.gold.linz_title_search AS
SELECT * FROM main.gold.linz_title_search_core;

-- Quick sanity check
SELECT 
  COUNT(*) AS gold_core_rows,
  SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_titles
FROM main.gold.linz_title_search_core;
