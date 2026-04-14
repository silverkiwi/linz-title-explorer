-- Build additional 7-layer + 14-layer tables into main.silver and main.gold
-- Requires existing main.bronze tables loaded via one-table job script.

USE CATALOG main;

CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.gold;

-- ============================================================
-- 1) Verify required additional BRONZE tables exist
-- ============================================================
CREATE OR REPLACE TEMP VIEW required_extra_bronze_tables AS
SELECT * FROM VALUES
  ('linz_appellation_raw'),
  ('linz_legal_description_raw'),
  ('linz_legal_description_parcel_raw'),
  ('linz_statute_raw'),
  ('linz_statute_action_raw'),
  ('linz_statutory_action_parcel_raw'),
  ('linz_title_parcel_association_raw'),
  ('aims_address_class_deprecated_raw'),
  ('aims_address_component_deprecated_raw'),
  ('aims_address_component_type_deprecated_raw'),
  ('aims_address_deprecated_raw'),
  ('aims_address_lifecycle_stage_deprecated_raw'),
  ('aims_address_position_type_deprecated_raw'),
  ('aims_address_reference_deprecated_raw'),
  ('aims_address_reference_object_type_deprecated_raw'),
  ('aims_addressable_object_deprecated_raw'),
  ('aims_addressable_object_external_deprecated_raw'),
  ('aims_addressable_object_lifecycle_stage_deprecated_raw'),
  ('aims_addressable_object_type_deprecated_raw'),
  ('aims_alternative_address_type_deprecated_raw'),
  ('aims_organisation_deprecated_raw')
AS t(table_name);

CREATE OR REPLACE TEMP VIEW missing_extra_bronze_tables AS
SELECT r.table_name
FROM required_extra_bronze_tables r
LEFT JOIN main.information_schema.tables i
  ON i.table_schema = 'bronze'
 AND i.table_name = r.table_name
WHERE i.table_name IS NULL;

SELECT * FROM missing_extra_bronze_tables;

SELECT assert_true(
  COUNT(*) = 0,
  CONCAT('Missing required extra bronze tables in main.bronze: ', CONCAT_WS(', ', COLLECT_LIST(table_name)))
)
FROM missing_extra_bronze_tables;

-- ============================================================
-- 2) Build additional SILVER tables (mapped to LDS dictionary sections)
-- ============================================================
-- 4.10 Appellation
CREATE OR REPLACE TABLE main.silver.linz_appellation AS
SELECT DISTINCT
  CAST(id AS BIGINT)                          AS id,
  CAST(par_id AS BIGINT)                      AS par_id,
  CAST(type AS STRING)                        AS type,
  CAST(title AS STRING)                       AS title,
  CAST(survey AS STRING)                      AS survey,
  CAST(status AS STRING)                      AS status,
  CAST(part_indic AS STRING)                  AS part_indicator,
  NULLIF(CAST(maori_name AS STRING), '')      AS maori_name,
  CAST(sub_type AS STRING)                    AS sub_type,
  NULLIF(CAST(appellatio AS STRING), '')      AS appellation_value,
  CAST(parcel_typ AS STRING)                  AS parcel_type,
  NULLIF(CAST(parcel_val AS STRING), '')      AS parcel_value,
  CAST(second_par AS STRING)                  AS second_parcel_type,
  NULLIF(CAST(second_prc AS STRING), '')      AS second_parcel_value,
  NULLIF(CAST(block_numb AS STRING), '')      AS block_number,
  CAST(sub_type_p AS STRING)                  AS sub_type_position,
  NULLIF(CAST(other_appe AS STRING), '')      AS other_appellation,
  CAST(act_id_crt AS BIGINT)                  AS act_id_crt,
  CAST(act_tin_id AS BIGINT)                  AS act_tin_id_crt,
  CAST(act_id_ext AS BIGINT)                  AS act_id_ext,
  CAST(act_tin__1 AS BIGINT)                  AS act_tin_id_ext,
  CAST(audit_id AS BIGINT)                    AS audit_id,
  CAST(height_lim AS STRING)                  AS height_limited,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_appellation_raw;

-- 4.26 Legal Description
CREATE OR REPLACE TABLE main.silver.linz_legal_description AS
SELECT DISTINCT
  CAST(id AS BIGINT)                          AS id,
  CAST(type AS STRING)                        AS type,
  CAST(status AS STRING)                      AS status,
  CAST(total_area AS DECIMAL(20,4))           AS total_area,
  NULLIF(CAST(ttl_title_ AS STRING), '')      AS ttl_title_no,
  NULLIF(CAST(legal_desc AS STRING), '')      AS legal_description,
  CAST(audit_id AS BIGINT)                    AS audit_id,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_legal_description_raw;

-- 4.27 Legal Description Parcel
CREATE OR REPLACE TABLE main.silver.linz_legal_description_parcel AS
SELECT DISTINCT
  CAST(lgd_id AS BIGINT)                      AS lgd_id,
  CAST(par_id AS BIGINT)                      AS par_id,
  CAST(sequence AS BIGINT)                    AS sequence,
  CAST(part_affec AS STRING)                  AS part_affected,
  NULLIF(CAST(share AS STRING), '')           AS share,
  CAST(audit_id AS BIGINT)                    AS audit_id,
  CAST(sur_wrk_id AS BIGINT)                  AS sur_wrk_id,
  source_extract_date,
  ingested_at
FROM main.bronze.linz_legal_description_parcel_raw;

-- 4.64 Statute
CREATE OR REPLACE TABLE main.silver.linz_statute AS
SELECT DISTINCT
  CAST(id AS BIGINT)                          AS id,
  NULLIF(CAST(section AS STRING), '')         AS section,
  NULLIF(CAST(name_and_d AS STRING), '')      AS name_and_description,
  CAST(still_in_f AS STRING)                  AS still_in_force,
  TO_DATE(in_force_d)                         AS in_force_date,
  TO_DATE(repeal_dat)                         AS repeal_date,
  CAST(type AS STRING)                        AS type,
  CAST(default AS STRING)                     AS default_flag,
  CAST(audit_id AS BIGINT)                    AS audit_id,
  source_extract_date,
  ingested_at
FROM main.bronze.linz_statute_raw;

-- 4.65 Statute Action
CREATE OR REPLACE TABLE main.silver.linz_statute_action AS
SELECT DISTINCT
  CAST(id AS BIGINT)                          AS id,
  CAST(type AS STRING)                        AS type,
  CAST(status AS STRING)                      AS status,
  CAST(ste_id AS BIGINT)                      AS ste_id,
  CAST(sur_wrk_id AS BIGINT)                  AS sur_wrk_id,
  CAST(gazette_ye AS BIGINT)                  AS gazette_year,
  CAST(gazette_pa AS BIGINT)                  AS gazette_page,
  CAST(gazette_ty AS STRING)                  AS gazette_type,
  NULLIF(CAST(other_lega AS STRING), '')      AS other_legal_description,
  TO_DATE(recorded_d)                         AS recorded_date,
  NULLIF(CAST(gazette_no AS STRING), '')      AS gazette_number,
  CAST(audit_id AS BIGINT)                    AS audit_id,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_statute_action_raw;

-- 4.66 Statutory Action Parcel
CREATE OR REPLACE TABLE main.silver.linz_statutory_action_parcel AS
SELECT DISTINCT
  CAST(sta_id AS BIGINT)                      AS sta_id,
  CAST(par_id AS BIGINT)                      AS par_id,
  CAST(status AS STRING)                      AS status,
  CAST(action AS STRING)                      AS action,
  NULLIF(CAST(purpose AS STRING), '')         AS purpose,
  NULLIF(CAST(name AS STRING), '')            AS name,
  NULLIF(CAST(comments AS STRING), '')        AS comments,
  CAST(audit_id AS BIGINT)                    AS audit_id,
  CAST(img_id AS BIGINT)                      AS img_id,
  NULLIF(CAST(descriptio AS STRING), '')      AS description,
  source_extract_date,
  ingested_at,
  CASE WHEN status IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END AS is_current
FROM main.bronze.linz_statutory_action_parcel_raw;

-- 4.83 Title Parcel Association
CREATE OR REPLACE TABLE main.silver.linz_title_parcel_association AS
SELECT DISTINCT
  CAST(id AS BIGINT)                          AS id,
  NULLIF(CAST(ttl_title_ AS STRING), '')      AS ttl_title_no,
  CAST(par_id AS BIGINT)                      AS par_id,
  CAST(source AS STRING)                      AS source,
  source_extract_date,
  ingested_at
FROM main.bronze.linz_title_parcel_association_raw;

-- AIMS deprecated address tables (not in LDS section 4.*, external AIMS domain)
CREATE OR REPLACE TABLE main.silver.aims_address_deprecated AS
SELECT DISTINCT
  CAST(address_id AS BIGINT)                  AS address_id,
  CAST(change_id AS BIGINT)                   AS change_id,
  CAST(primary_ad AS STRING)                  AS primary_address,
  CAST(address_li AS STRING)                  AS address_lifecycle_stage,
  CAST(address_pr AS STRING)                  AS address_position_type,
  CAST(address_ma AS STRING)                  AS alternative_address_type,
  CAST(addressabl AS BIGINT)                  AS addressable_object_id,
  CAST(address_cl AS STRING)                  AS address_class,
  CAST(parcel_id AS BIGINT)                   AS parcel_id,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_address_component_deprecated AS
SELECT DISTINCT
  CAST(address_co AS BIGINT)                  AS address_component_id,
  CAST(address_id AS BIGINT)                  AS address_id,
  CAST(address__1 AS STRING)                  AS component_type,
  NULLIF(CAST(address__2 AS STRING), '')      AS component_value,
  CAST(address__3 AS BIGINT)                  AS sequence,
  CAST(address__4 AS STRING)                  AS language_code,
  TO_DATE(begin_life)                         AS begin_lifespan,
  TO_DATE(end_lifesp)                         AS end_lifespan,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_component_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_address_component_type_deprecated AS
SELECT DISTINCT
  CAST(address_co AS STRING)                  AS component_type,
  NULLIF(CAST(address__1 AS STRING), '')      AS description,
  CAST(address__2 AS STRING)                  AS active_flag,
  NULLIF(CAST(address__3 AS STRING), '')      AS notes,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_component_type_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_address_reference_deprecated AS
SELECT DISTINCT
  CAST(address_re AS BIGINT)                  AS address_reference_id,
  CAST(address_id AS BIGINT)                  AS address_id,
  CAST(address__1 AS STRING)                  AS reference_object_type,
  NULLIF(CAST(address__2 AS STRING), '')      AS reference_value,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_reference_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_address_reference_object_type_deprecated AS
SELECT DISTINCT
  CAST(address_re AS STRING)                  AS reference_object_type,
  NULLIF(CAST(address__1 AS STRING), '')      AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_reference_object_type_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_address_class_deprecated AS
SELECT DISTINCT
  CAST(address_cl AS STRING)                  AS address_class,
  NULLIF(CAST(address__1 AS STRING), '')      AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_class_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_address_lifecycle_stage_deprecated AS
SELECT DISTINCT
  CAST(address_li AS STRING)                  AS address_lifecycle_stage,
  NULLIF(CAST(address__1 AS STRING), '')      AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_lifecycle_stage_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_address_position_type_deprecated AS
SELECT DISTINCT
  CAST(address_po AS STRING)                  AS address_position_type,
  NULLIF(CAST(address__1 AS STRING), '')      AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_address_position_type_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_addressable_object_deprecated AS
SELECT DISTINCT
  CAST(addressabl AS BIGINT)                  AS addressable_object_id,
  CAST(addressa_1 AS STRING)                  AS object_type,
  CAST(addressa_2 AS STRING)                  AS lifecycle_stage,
  CAST(organisati AS BIGINT)                  AS organisation_id,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_addressable_object_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_addressable_object_external_deprecated AS
SELECT DISTINCT
  CAST(addressabl AS BIGINT)                  AS addressable_object_id,
  CAST(addressa_1 AS STRING)                  AS object_type,
  CAST(addressa_2 AS STRING)                  AS lifecycle_stage,
  NULLIF(CAST(external_i AS STRING), '')      AS external_id,
  NULLIF(CAST(external_1 AS STRING), '')      AS external_system,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_addressable_object_external_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_addressable_object_lifecycle_stage_deprecated AS
SELECT DISTINCT
  CAST(addressabl AS STRING)                  AS lifecycle_stage,
  NULLIF(CAST(addressa_1 AS STRING), '')      AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_addressable_object_lifecycle_stage_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_addressable_object_type_deprecated AS
SELECT DISTINCT
  CAST(addressabl AS STRING)                  AS object_type,
  NULLIF(CAST(addressa_1 AS STRING), '')      AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_addressable_object_type_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_alternative_address_type_deprecated AS
SELECT DISTINCT
  CAST(alternativ AS STRING)                  AS alternative_address_type,
  NULLIF(CAST(alternat_1 AS STRING), '')      AS description,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_alternative_address_type_deprecated_raw;

CREATE OR REPLACE TABLE main.silver.aims_organisation_deprecated AS
SELECT DISTINCT
  CAST(organisati AS BIGINT)                  AS organisation_id,
  NULLIF(CAST(organisa_1 AS STRING), '')      AS organisation_name,
  CAST(address_id AS BIGINT)                  AS address_id,
  CAST(addressabl AS BIGINT)                  AS addressable_object_id,
  source_extract_date,
  ingested_at
FROM main.bronze.aims_organisation_deprecated_raw;

-- ============================================================
-- 3) Build new GOLD integration tables
-- ============================================================
-- Title + parcel + legal description + appellation enriched view
CREATE OR REPLACE TABLE main.gold.linz_title_search_enriched AS
WITH base AS (
  SELECT * FROM main.gold.linz_title_search
),
parcel_agg AS (
  SELECT
    tpa.ttl_title_no AS title_no,
    COUNT(DISTINCT tpa.par_id) AS parcel_count,
    sort_array(collect_set(tpa.par_id)) AS parcel_ids
  FROM main.silver.linz_title_parcel_association tpa
  GROUP BY tpa.ttl_title_no
),
app_agg AS (
  SELECT
    tpa.ttl_title_no AS title_no,
    COUNT(DISTINCT a.id) AS appellation_count,
    concat_ws(' | ', slice(sort_array(collect_set(concat_ws(' ', a.parcel_type, a.parcel_value, a.appellation_value))), 1, 8)) AS appellation_preview,
    concat_ws(' ',
      concat_ws(' ', collect_set(a.appellation_value)),
      concat_ws(' ', collect_set(a.parcel_value)),
      concat_ws(' ', collect_set(a.block_number))
    ) AS appellation_search_text
  FROM main.silver.linz_title_parcel_association tpa
  LEFT JOIN main.silver.linz_appellation a
    ON tpa.par_id = a.par_id
  GROUP BY tpa.ttl_title_no
),
legal_agg AS (
  SELECT
    coalesce(ld.ttl_title_no, tpa.ttl_title_no) AS title_no,
    COUNT(DISTINCT ld.id) AS legal_description_count,
    concat_ws(' | ', slice(sort_array(collect_set(ld.legal_description)), 1, 8)) AS legal_description_preview,
    concat_ws(' ', collect_set(ld.legal_description)) AS legal_description_search_text
  FROM main.silver.linz_legal_description ld
  LEFT JOIN main.silver.linz_legal_description_parcel ldp
    ON ld.id = ldp.lgd_id
  LEFT JOIN main.silver.linz_title_parcel_association tpa
    ON ldp.par_id = tpa.par_id
  GROUP BY coalesce(ld.ttl_title_no, tpa.ttl_title_no)
)
SELECT
  b.*, 
  coalesce(p.parcel_count, 0) AS parcel_count,
  coalesce(a.appellation_count, 0) AS appellation_count,
  coalesce(l.legal_description_count, 0) AS legal_description_count,
  a.appellation_preview,
  l.legal_description_preview,
  concat_ws(' ',
    b.search_text,
    coalesce(a.appellation_search_text, ''),
    coalesce(l.legal_description_search_text, '')
  ) AS search_text_enriched,
  p.parcel_ids,
  current_timestamp() AS enriched_updated_at
FROM base b
LEFT JOIN parcel_agg p ON b.title_no = p.title_no
LEFT JOIN app_agg a ON b.title_no = a.title_no
LEFT JOIN legal_agg l ON b.title_no = l.title_no;

OPTIMIZE main.gold.linz_title_search_enriched
ZORDER BY (title_no, title_status, latest_lodged_date);

-- Deprecated AIMS address search projection (text-only; no geometry in this package)
CREATE OR REPLACE TABLE main.gold.aims_address_search_deprecated AS
WITH component_text AS (
  SELECT
    address_id,
    concat_ws(' ', filter(collect_list(component_value), x -> x IS NOT NULL)) AS component_text,
    concat_ws(' ', filter(collect_list(component_type), x -> x IS NOT NULL)) AS component_types
  FROM main.silver.aims_address_component_deprecated
  GROUP BY address_id
),
ref_text AS (
  SELECT
    address_id,
    concat_ws(' ', filter(collect_list(reference_value), x -> x IS NOT NULL)) AS reference_text
  FROM main.silver.aims_address_reference_deprecated
  GROUP BY address_id
)
SELECT
  a.address_id,
  a.parcel_id,
  a.primary_address,
  a.address_class,
  a.address_lifecycle_stage,
  a.address_position_type,
  a.alternative_address_type,
  a.addressable_object_id,
  coalesce(c.component_text, '') AS component_text,
  coalesce(r.reference_text, '') AS reference_text,
  lower(concat_ws(' ',
    cast(a.address_id AS STRING),
    coalesce(c.component_text, ''),
    coalesce(r.reference_text, ''),
    cast(a.parcel_id AS STRING)
  )) AS search_text,
  a.source_extract_date,
  a.ingested_at,
  current_timestamp() AS updated_at
FROM main.silver.aims_address_deprecated a
LEFT JOIN component_text c ON a.address_id = c.address_id
LEFT JOIN ref_text r ON a.address_id = r.address_id;

OPTIMIZE main.gold.aims_address_search_deprecated
ZORDER BY (address_id, parcel_id);

-- Optional sanity checks
SELECT COUNT(*) AS enriched_titles FROM main.gold.linz_title_search_enriched;
SELECT COUNT(*) AS deprecated_addresses FROM main.gold.aims_address_search_deprecated;
