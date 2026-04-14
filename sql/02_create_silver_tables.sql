CREATE OR REPLACE TABLE silver.linz_title AS
SELECT
  CAST(title_no AS STRING)               AS title_no,
  CAST(ldt_loc_id AS BIGINT)             AS ldt_loc_id,
  CAST(register_t AS STRING)             AS register_type,
  CAST(ste_id AS BIGINT)                 AS ste_id,
  CAST(issue_date AS DATE)               AS issue_date,
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
FROM bronze.linz_title_raw;

CREATE OR REPLACE TABLE silver.linz_title_estate AS
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
FROM bronze.linz_title_estate_raw;

CREATE OR REPLACE TABLE silver.linz_title_instrument AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(inst_no AS STRING)             AS inst_no,
  CAST(trt_grp AS STRING)             AS trt_grp,
  CAST(trt_type AS STRING)            AS trt_type,
  CAST(ldt_loc_id AS BIGINT)          AS ldt_loc_id,
  CAST(status AS STRING)              AS status,
  CAST(lodged_dat AS DATE)            AS lodged_datetime,
  CAST(dlg_id AS BIGINT)              AS dlg_id,
  CAST(priority_n AS BIGINT)          AS priority_no,
  CAST(tin_id_par AS BIGINT)          AS tin_id_parent,
  CAST(audit_id AS BIGINT)            AS audit_id,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM bronze.linz_title_instrument_raw;

CREATE OR REPLACE TABLE silver.linz_title_instrument_title AS
SELECT
  CAST(tin_id AS BIGINT)      AS tin_id,
  CAST(ttl_title_ AS STRING)  AS ttl_title_no,
  CAST(audit_id AS BIGINT)    AS audit_id
FROM bronze.linz_title_instrument_title_raw;

CREATE OR REPLACE TABLE silver.linz_title_hierarchy AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(status AS STRING)              AS status,
  NULLIF(CAST(ttl_title_ AS STRING), '') AS ttl_title_no_prior,
  NULLIF(CAST(ttl_titl_1 AS STRING), '') AS ttl_title_no_flw,
  CAST(tdr_id AS BIGINT)              AS tdr_id,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM bronze.linz_title_hierarchy_raw;

CREATE OR REPLACE TABLE silver.linz_encumbrance AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(status AS STRING)              AS status,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_tin__1 AS BIGINT)          AS act_tin_id_orig,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  CAST(act_id_ori AS BIGINT)          AS act_id_orig,
  NULLIF(CAST(term AS STRING), '')    AS term,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM bronze.linz_encumbrance_raw;

CREATE OR REPLACE TABLE silver.linz_title_encumbrance AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(ttl_title_ AS STRING)          AS ttl_title_no,
  CAST(enc_id AS BIGINT)              AS enc_id,
  CAST(status AS STRING)              AS status,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM bronze.linz_title_encumbrance_raw;

CREATE OR REPLACE TABLE silver.linz_encumbrance_share AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(enc_id AS BIGINT)              AS enc_id,
  CAST(status AS STRING)              AS status,
  CAST(act_tin_id AS BIGINT)          AS act_tin_id_crt,
  CAST(act_id_crt AS BIGINT)          AS act_id_crt,
  CAST(act_id_ext AS BIGINT)          AS act_id_ext,
  CAST(act_tin__1 AS BIGINT)          AS act_tin_id_ext,
  NULLIF(CAST(system_crt AS STRING), '') AS system_created,
  NULLIF(CAST(system_ext AS STRING), '') AS system_extinguished,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM bronze.linz_encumbrance_share_raw;

CREATE OR REPLACE TABLE silver.linz_title_document_reference AS
SELECT
  CAST(id AS BIGINT)                  AS id,
  CAST(type AS STRING)                AS type,
  CAST(tin_id AS BIGINT)              AS tin_id,
  NULLIF(CAST(reference_ AS STRING), '') AS reference
FROM bronze.linz_title_document_reference_raw;

CREATE OR REPLACE TABLE silver.linz_transaction_type AS
SELECT
  CAST(grp AS STRING)                 AS grp,
  CAST(type AS STRING)                AS type,
  NULLIF(CAST(descriptio AS STRING), '') AS description,
  CAST(audit_id AS BIGINT)            AS audit_id
FROM bronze.linz_transaction_type_raw;
