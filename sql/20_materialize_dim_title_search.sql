-- ============================================================
-- 20_materialize_dim_title_search.sql
-- Materialise GOLD.V_TITLE_360 as a physical table so that
-- title searches are near-instant instead of re-running the
-- full multi-table aggregation (8 CTEs across SILVER) on
-- every query.
--
-- Three complementary optimisations mirror the approach used
-- for DIM_ADDRESS_SEARCH (see 16_materialize_dim_address.sql):
--
--   1. Physical table  — eliminates the per-query CTE cost
--      (estate, ownership, instr, enc, par, addr, appel,
--       lineage) that V_TITLE_360 computes on the fly.
--
--   2. Pre-normalised columns (lowercase + macrons stripped)
--      — avoids per-row LOWER(TRANSLATE(...)) at query time,
--        which is what lets Search Optimization fire.
--
--   3. Search Optimization (SUBSTRING) on all five normalised
--      search columns — turns leading-wildcard LIKE queries
--      into O(log n) index lookups instead of full-table scans.
--
-- Additionally, a clustering key on (IS_CURRENT, TITLE_NO):
--   • queries that ORDER BY IS_CURRENT DESC prune most of the
--     table to only the current-title micro-partitions
--   • point-lookups by TITLE_NO are physically co-located
--
-- Run once after initial load; thereafter the TASK
-- OPS.REFRESH_DIM_TITLE_SEARCH keeps the table current.
-- ============================================================

USE DATABASE L;
USE SCHEMA GOLD;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1) Materialised search table
-- ============================================================
CREATE OR REPLACE TABLE GOLD.DIM_TITLE_SEARCH AS
SELECT
    TITLE_NO,
    TITLE_STATUS,
    TITLE_STATUS_DESC,
    TITLE_TYPE,
    TITLE_TYPE_DESC,
    REGISTER_TYPE,
    REGISTER_TYPE_DESC,
    ISSUE_DATE,
    IS_CURRENT,
    IS_ACTIVE,
    GUARANTEE_STATUS,
    MAORI_LAND,
    LDT_LOC_ID,
    LAND_DISTRICT_NAME,
    PROPRIETOR_COUNT,
    PROPRIETORS,
    ESTATE_COUNT,
    CURRENT_ESTATE_COUNT,
    ESTATE_TYPES,
    INSTRUMENT_COUNT,
    LATEST_INSTRUMENT_DATE,
    LATEST_INST_NO,
    LATEST_INST_TYPE,
    LATEST_INST_TYPE_DESC,
    ENCUMBRANCE_COUNT,
    CURRENT_ENCUMBRANCE_COUNT,
    PARCEL_COUNT,
    PRIMARY_ADDRESS,
    APPELLATIONS,
    PRIOR_TITLE_NO,
    REFRESHED_AT,
    -- Pre-normalised: lowercase + Māori macrons → ASCII.
    -- These are the columns searched at query time; storing them
    -- avoids LOWER(TRANSLATE(...)) on every row and lets the
    -- Search Optimization index fire on LIKE '%substring%'.
    LOWER(TITLE_NO)                                                                  AS TITLE_NO_NORM,
    LOWER(TRANSLATE(COALESCE(PRIMARY_ADDRESS, ''), 'āēīōūĀĒĪŌŪ', 'aeiouAEIOU'))     AS PRIMARY_ADDRESS_NORM,
    LOWER(TRANSLATE(COALESCE(APPELLATIONS,    ''), 'āēīōūĀĒĪŌŪ', 'aeiouAEIOU'))     AS APPELLATIONS_NORM,
    LOWER(TRANSLATE(COALESCE(PROPRIETORS,     ''), 'āēīōūĀĒĪŌŪ', 'aeiouAEIOU'))     AS PROPRIETORS_NORM,
    LOWER(COALESCE(ESTATE_TYPES, ''))                                                AS ESTATE_TYPES_NORM
FROM GOLD.V_TITLE_360;

-- ============================================================
-- 2) Clustering key — prunes micro-partitions for the common
--    ORDER BY IS_CURRENT DESC ... TITLE_NO ASC access pattern,
--    and co-locates rows for point-lookup by TITLE_NO.
-- ============================================================
ALTER TABLE GOLD.DIM_TITLE_SEARCH
    CLUSTER BY (IS_CURRENT, TITLE_NO);

-- ============================================================
-- 3) Search Optimization — builds a server-side search access
--    path that makes LIKE '%substring%' queries O(log n).
--    All five normalised search columns are covered.
-- ============================================================
ALTER TABLE GOLD.DIM_TITLE_SEARCH
    ADD SEARCH OPTIMIZATION ON
        SUBSTRING(TITLE_NO_NORM),
        SUBSTRING(PRIMARY_ADDRESS_NORM),
        SUBSTRING(APPELLATIONS_NORM),
        SUBSTRING(PROPRIETORS_NORM),
        SUBSTRING(ESTATE_TYPES_NORM);

-- ============================================================
-- 4) Scheduled refresh task
--    Runs nightly at 03:30 NZST (15:30 UTC), 30 min after
--    DIM_ADDRESS_SEARCH refreshes, to pick up new LINZ data.
--    Tasks start SUSPENDED — RESUME is called below.
-- ============================================================
-- NOTE: INSERT OVERWRITE INTO (not CREATE OR REPLACE TABLE) so
-- that the clustering key and Search Optimization index are
-- preserved across every nightly refresh.  CREATE OR REPLACE
-- TABLE would silently drop the table object, losing them.
CREATE OR REPLACE TASK OPS.REFRESH_DIM_TITLE_SEARCH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 30 15 * * * UTC'
    COMMENT   = 'Nightly rebuild of GOLD.DIM_TITLE_SEARCH (03:30 NZST)'
AS
    INSERT OVERWRITE INTO GOLD.DIM_TITLE_SEARCH
    SELECT
        TITLE_NO,
        TITLE_STATUS,
        TITLE_STATUS_DESC,
        TITLE_TYPE,
        TITLE_TYPE_DESC,
        REGISTER_TYPE,
        REGISTER_TYPE_DESC,
        ISSUE_DATE,
        IS_CURRENT,
        IS_ACTIVE,
        GUARANTEE_STATUS,
        MAORI_LAND,
        LDT_LOC_ID,
        LAND_DISTRICT_NAME,
        PROPRIETOR_COUNT,
        PROPRIETORS,
        ESTATE_COUNT,
        CURRENT_ESTATE_COUNT,
        ESTATE_TYPES,
        INSTRUMENT_COUNT,
        LATEST_INSTRUMENT_DATE,
        LATEST_INST_NO,
        LATEST_INST_TYPE,
        LATEST_INST_TYPE_DESC,
        ENCUMBRANCE_COUNT,
        CURRENT_ENCUMBRANCE_COUNT,
        PARCEL_COUNT,
        PRIMARY_ADDRESS,
        APPELLATIONS,
        PRIOR_TITLE_NO,
        REFRESHED_AT,
        LOWER(TITLE_NO)                                                                  AS TITLE_NO_NORM,
        LOWER(TRANSLATE(COALESCE(PRIMARY_ADDRESS, ''), 'āēīōūĀĒĪŌŪ', 'aeiouAEIOU'))     AS PRIMARY_ADDRESS_NORM,
        LOWER(TRANSLATE(COALESCE(APPELLATIONS,    ''), 'āēīōūĀĒĪŌŪ', 'aeiouAEIOU'))     AS APPELLATIONS_NORM,
        LOWER(TRANSLATE(COALESCE(PROPRIETORS,     ''), 'āēīōūĀĒĪŌŪ', 'aeiouAEIOU'))     AS PROPRIETORS_NORM,
        LOWER(COALESCE(ESTATE_TYPES, ''))                                                AS ESTATE_TYPES_NORM
    FROM GOLD.V_TITLE_360;

ALTER TASK OPS.REFRESH_DIM_TITLE_SEARCH RESUME;

-- ============================================================
-- 5) Smoke check
-- ============================================================
SELECT
    'DIM_TITLE_SEARCH row count'    AS check_name,
    COUNT(*)                        AS row_count
FROM GOLD.DIM_TITLE_SEARCH

UNION ALL

SELECT
    'search optimisation status',
    COUNT(*)
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    TABLE_NAME       => 'DIM_TITLE_SEARCH'
));
