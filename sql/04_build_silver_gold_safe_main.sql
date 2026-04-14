-- Safe Silver + Gold build (fully qualified, with dependency checks)
-- Catalog: main

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
-- 2) Build SILVER tables
--    Note: bronze ingested as STRINGs; cast/parse in silver.
-- ============================================================
CREATE OR REPLACE TABLE main.silver.linz_title AS
SELECT
  CAST(title_no AS STRING)               AS title_no,
  CAST(ldt_loc_id AS BIGINT)             AS ldt_loc_id,
  CAST(register_t AS STRING)             AS register_type,
  CAST(ste_id AS BIGINT)                 AS ste_id,
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
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_estate AS
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
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_estate_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_instrument AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(inst_no AS STRING)             AS inst_no,
  CAST(trt_grp AS STRING)             AS trt_grp,
  CAST(trt_type AS STRING)            AS trt_type,
  CAST(ldt_loc_id AS BIGINT)          AS ldt_loc_id,
  CAST(status AS STRING)              AS status,
  TO_DATE(lodged_dat)                 AS lodged_datetime,
  CAST(dlg_id AS BIGINT)              AS dlg_id,
  CAST(priority_n AS BIGINT)          AS priority_no,
  CAST(tin_id_par AS BIGINT)          AS tin_id_parent,
  CAST(audit_id AS BIGINT)            AS audit_id,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_instrument_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_instrument_title AS
SELECT
  CAST(tin_id AS BIGINT)      AS tin_id,
  CAST(ttl_title_ AS STRING)  AS ttl_title_no,
  CAST(audit_id AS BIGINT)    AS audit_id
FROM main.bronze.linz_title_instrument_title_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_hierarchy AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(status AS STRING)                 AS status,
  NULLIF(CAST(ttl_title_ AS STRING), '') AS ttl_title_no_prior,
  NULLIF(CAST(ttl_titl_1 AS STRING), '') AS ttl_title_no_flw,
  CAST(tdr_id AS BIGINT)                 AS tdr_id,
  CAST(act_tin_id AS BIGINT)             AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)             AS act_id_crt,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_hierarchy_raw;

CREATE OR REPLACE TABLE main.silver.linz_encumbrance AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(status AS STRING)              AS status,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_tin__1 AS BIGINT)          AS act_tin_id_orig,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  CAST(act_id_ori AS BIGINT)          AS act_id_orig,
  NULLIF(CAST(term AS STRING), '')    AS term,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_encumbrance_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_encumbrance AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(ttl_title_ AS STRING)          AS ttl_title_no,
  CAST(enc_id AS BIGINT)              AS enc_id,
  CAST(status AS STRING)              AS status,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_title_encumbrance_raw;

CREATE OR REPLACE TABLE main.silver.linz_encumbrance_share AS
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
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_encumbrance_share_raw;

CREATE OR REPLACE TABLE main.silver.linz_title_document_reference AS
SELECT
  CAST(id AS BIGINT)                     AS id,
  CAST(type AS STRING)                   AS type,
  CAST(tin_id AS BIGINT)                 AS tin_id,
  NULLIF(CAST(reference_ AS STRING), '') AS reference
FROM main.bronze.linz_title_document_reference_raw;

CREATE OR REPLACE TABLE main.silver.linz_transaction_type AS
SELECT
  CAST(grp AS STRING)                    AS grp,
  CAST(type AS STRING)                   AS type,
  NULLIF(CAST(descriptio AS STRING), '') AS description,
  CAST(audit_id AS BIGINT)               AS audit_id
FROM main.bronze.linz_transaction_type_raw;

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
-- 4) Build GOLD serving table
-- ============================================================
CREATE OR REPLACE TABLE main.gold.linz_title_search AS
WITH estate_agg AS (
  SELECT
    ttl_title_no,
    COUNT(*) AS estate_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_estate_count,
    element_at(sort_array(collect_set(CASE WHEN is_current THEN type END)), 1) AS primary_estate_type,
    sort_array(filter(collect_set(type), x -> x IS NOT NULL)) AS estate_types,
    concat_ws('; ', filter(collect_list(share), x -> x IS NOT NULL)) AS estate_share_summary,
    concat_ws('; ', filter(collect_list(purpose), x -> x IS NOT NULL)) AS estate_purpose_summary,
    max(CASE WHEN timeshare_week_no IS NOT NULL THEN TRUE ELSE FALSE END) AS has_timeshare_estate,
    max(CASE WHEN term IS NOT NULL THEN TRUE ELSE FALSE END) AS has_term_estate,
    collect_list(named_struct(
      'id', id,
      'type', type,
      'status', status,
      'share', share,
      'purpose', purpose,
      'term', term,
      'original_flag', original_flag
    )) AS estates
  FROM main.silver.linz_title_estate
  GROUP BY ttl_title_no
),
instrument_detail AS (
  SELECT
    tit.ttl_title_no,
    ti.id AS tin_id,
    ti.inst_no,
    ti.trt_grp,
    ti.trt_type,
    tt.description AS transaction_type_description,
    ti.status,
    ti.lodged_datetime,
    ti.priority_no
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
    max_by(tin_id, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_id,
    max_by(inst_no, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_no,
    max_by(trt_type, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_type,
    max_by(transaction_type_description, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_description,
    max(lodged_datetime) AS latest_lodged_date,
    collect_list(named_struct(
      'tin_id', tin_id,
      'inst_no', inst_no,
      'trt_grp', trt_grp,
      'trt_type', trt_type,
      'description', transaction_type_description,
      'status', status,
      'lodged_datetime', lodged_datetime,
      'priority_no', priority_no
    )) AS instruments
  FROM instrument_detail
  GROUP BY ttl_title_no
),
encumbrance_agg AS (
  SELECT
    te.ttl_title_no,
    COUNT(*) AS encumbrance_count,
    SUM(CASE WHEN te.is_current THEN 1 ELSE 0 END) AS current_encumbrance_count,
    MAX(CASE WHEN te.is_current THEN TRUE ELSE FALSE END) AS has_current_encumbrance,
    max(te.enc_id) AS latest_encumbrance_id,
    collect_list(named_struct(
      'title_encumbrance_id', te.id,
      'enc_id', te.enc_id,
      'status', te.status,
      'act_tin_id_crt', te.act_tin_id_crt,
      'act_id_crt', te.act_id_crt,
      'encumbrance_status', e.status,
      'term', e.term
    )) AS encumbrances
  FROM main.silver.linz_title_encumbrance te
  LEFT JOIN main.silver.linz_encumbrance e
    ON te.enc_id = e.id
  GROUP BY te.ttl_title_no
),
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
),
doc_ref_agg AS (
  SELECT
    tit.ttl_title_no,
    COUNT(*) AS document_reference_count,
    collect_list(named_struct(
      'id', dr.id,
      'type', dr.type,
      'tin_id', dr.tin_id,
      'reference', dr.reference
    )) AS document_references
  FROM main.silver.linz_title_instrument_title tit
  JOIN main.silver.linz_title_document_reference dr
    ON tit.tin_id = dr.tin_id
  GROUP BY tit.ttl_title_no
)
SELECT
  t.title_no,
  t.status AS title_status,
  t.type AS title_type,
  t.register_type,
  t.issue_date,
  t.guarantee_status,
  t.provisional,
  t.maori_land,
  t.ste_id,
  t.sur_wrk_id,
  t.ldt_loc_id,
  t.ttl_title_no_srs,
  t.ttl_title_no_head_srs,
  t.is_current,
  CASE WHEN t.status = 'LIVE' THEN TRUE ELSE FALSE END AS is_live,
  coalesce(e.estate_count, 0) AS estate_count,
  coalesce(e.current_estate_count, 0) AS current_estate_count,
  e.primary_estate_type,
  coalesce(e.estate_types, array()) AS estate_types,
  e.estate_share_summary,
  e.estate_purpose_summary,
  coalesce(e.has_timeshare_estate, FALSE) AS has_timeshare_estate,
  coalesce(e.has_term_estate, FALSE) AS has_term_estate,
  coalesce(e.estates, array()) AS estates,
  coalesce(i.instrument_count, 0) AS instrument_count,
  coalesce(i.registered_instrument_count, 0) AS registered_instrument_count,
  i.latest_instrument_id,
  i.latest_instrument_no,
  i.latest_instrument_type,
  i.latest_instrument_description,
  i.latest_lodged_date,
  coalesce(i.instruments, array()) AS instruments,
  coalesce(enc.encumbrance_count, 0) AS encumbrance_count,
  coalesce(enc.current_encumbrance_count, 0) AS current_encumbrance_count,
  coalesce(enc.has_current_encumbrance, FALSE) AS has_current_encumbrance,
  enc.latest_encumbrance_id,
  coalesce(enc.encumbrances, array()) AS encumbrances,
  coalesce(p.prior_title_count, 0) AS prior_title_count,
  coalesce(f.follow_on_title_count, 0) AS follow_on_title_count,
  coalesce(p.prior_titles, array()) AS prior_titles,
  coalesce(f.follow_on_titles, array()) AS follow_on_titles,
  CASE
    WHEN coalesce(p.prior_title_count, 0) > 0 OR coalesce(f.follow_on_title_count, 0) > 0 THEN TRUE
    ELSE FALSE
  END AS has_title_lineage,
  coalesce(d.document_reference_count, 0) AS document_reference_count,
  coalesce(d.document_references, array()) AS document_references,
  concat_ws(' ',
    t.title_no,
    t.type,
    t.register_type,
    coalesce(i.latest_instrument_no, ''),
    coalesce(i.latest_instrument_description, '')
  ) AS search_text,
  current_timestamp() AS updated_at
FROM main.silver.linz_title t
LEFT JOIN estate_agg e ON t.title_no = e.ttl_title_no
LEFT JOIN instrument_agg i ON t.title_no = i.ttl_title_no
LEFT JOIN encumbrance_agg enc ON t.title_no = enc.ttl_title_no
LEFT JOIN prior_agg p ON t.title_no = p.title_no
LEFT JOIN follow_on_agg f ON t.title_no = f.title_no
LEFT JOIN doc_ref_agg d ON t.title_no = d.ttl_title_no;

OPTIMIZE main.gold.linz_title_search
ZORDER BY (title_no, title_status, title_type, latest_instrument_no, latest_lodged_date);

-- Optional quick sanity check
SELECT COUNT(*) AS gold_rows FROM main.gold.linz_title_search;
