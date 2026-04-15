-- ============================================================
-- 18_validate_build.sql
-- Smoke checks to verify the LINZ Snowflake build is complete
-- and correct.  Run standalone or via scripts/snowsql_build.sh.
-- ============================================================

USE DATABASE L;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1) Schema inventory — expected objects
-- ============================================================

SELECT 'SILVER tables' AS section, TABLE_NAME, COLUMN_COUNT
FROM (
    SELECT TABLE_NAME, COUNT(*) AS COLUMN_COUNT
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'SILVER'
    GROUP BY TABLE_NAME
)
ORDER BY TABLE_NAME;

SELECT 'GOLD views' AS section, TABLE_NAME
FROM   INFORMATION_SCHEMA.VIEWS
WHERE  TABLE_SCHEMA = 'GOLD'
ORDER BY TABLE_NAME;

-- ============================================================
-- 2) V_TITLE_360 column check
--    Fails if any of the app-required columns are missing.
-- ============================================================

SELECT
    CASE
        WHEN COUNT_IF(COLUMN_NAME = 'TITLE_STATUS_DESC')      = 0 THEN 'MISSING: TITLE_STATUS_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'TITLE_TYPE_DESC')         = 0 THEN 'MISSING: TITLE_TYPE_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'REGISTER_TYPE_DESC')      = 0 THEN 'MISSING: REGISTER_TYPE_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'LATEST_INST_TYPE')        = 0 THEN 'MISSING: LATEST_INST_TYPE'
        WHEN COUNT_IF(COLUMN_NAME = 'LATEST_INST_TYPE_DESC')   = 0 THEN 'MISSING: LATEST_INST_TYPE_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'PRIMARY_ADDRESS')         = 0 THEN 'MISSING: PRIMARY_ADDRESS'
        WHEN COUNT_IF(COLUMN_NAME = 'APPELLATIONS')            = 0 THEN 'MISSING: APPELLATIONS'
        WHEN COUNT_IF(COLUMN_NAME = 'PRIOR_TITLE_NO')          = 0 THEN 'MISSING: PRIOR_TITLE_NO'
        ELSE 'OK — all required V_TITLE_360 columns present'
    END AS v_title_360_check
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'GOLD'
  AND TABLE_NAME   = 'V_TITLE_360';

-- ============================================================
-- 3) SILVER table column checks
--    Ensures schema migrations in 09_create_loading... were applied.
-- ============================================================

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    'PRESENT' AS status
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'SILVER'
  AND (
        (TABLE_NAME = 'APPELLATION'              AND COLUMN_NAME IN ('HEIGHT_LIMITED','OTHER_APPELLATION'))
     OR (TABLE_NAME = 'LEGAL_DESCRIPTION'        AND COLUMN_NAME IN ('TTL_TITLE_NO','TOTAL_AREA','LEGAL_DESC_TEXT'))
     OR (TABLE_NAME = 'LEGAL_DESCRIPTION_PARCEL' AND COLUMN_NAME IN ('SEQUENCE','PART_AFFECTED','SHARE','SUR_WRK_ID'))
     OR (TABLE_NAME = 'TITLE_PARCEL_ASSOCIATION' AND COLUMN_NAME IN ('ID','SOURCE'))
     OR (TABLE_NAME = 'ENCUMBRANCE_SHARE'        AND COLUMN_NAME = 'ENC_ID')
  )
ORDER BY TABLE_NAME, COLUMN_NAME;

-- ============================================================
-- 4) Row counts — key tables
-- ============================================================

SELECT 'SILVER.TITLE'                AS table_name, COUNT(*) AS row_count FROM SILVER.TITLE
UNION ALL SELECT 'SILVER.TITLE_ESTATE',             COUNT(*) FROM SILVER.TITLE_ESTATE
UNION ALL SELECT 'SILVER.TITLE_INSTRUMENT',         COUNT(*) FROM SILVER.TITLE_INSTRUMENT
UNION ALL SELECT 'SILVER.TITLE_ENCUMBRANCE',        COUNT(*) FROM SILVER.TITLE_ENCUMBRANCE
UNION ALL SELECT 'SILVER.TITLE_PARCEL_ASSOCIATION', COUNT(*) FROM SILVER.TITLE_PARCEL_ASSOCIATION
UNION ALL SELECT 'SILVER.PROPRIETOR',               COUNT(*) FROM SILVER.PROPRIETOR
UNION ALL SELECT 'SILVER.APPELLATION',              COUNT(*) FROM SILVER.APPELLATION
UNION ALL SELECT 'GOLD.DIM_ADDRESS_SEARCH',         COUNT(*) FROM GOLD.DIM_ADDRESS_SEARCH
UNION ALL SELECT 'OPS.FILE_LOAD_CONTROL',           COUNT(*) FROM OPS.FILE_LOAD_CONTROL
ORDER BY table_name;

-- ============================================================
-- 5) V_TITLE_360 query test — fetch 5 rows including DESC cols
--    Catches runtime errors in the view (bad column refs, etc.)
-- ============================================================

SELECT
    TITLE_NO,
    TITLE_STATUS,    TITLE_STATUS_DESC,
    TITLE_TYPE,      TITLE_TYPE_DESC,
    REGISTER_TYPE,   REGISTER_TYPE_DESC,
    INSTRUMENT_COUNT,
    LATEST_INST_NO,  LATEST_INST_TYPE, LATEST_INST_TYPE_DESC,
    ENCUMBRANCE_COUNT,
    PRIMARY_ADDRESS,
    APPELLATIONS
FROM GOLD.V_TITLE_360
WHERE IS_ACTIVE = TRUE
ORDER BY INSTRUMENT_COUNT DESC NULLS LAST
LIMIT 5;

-- ============================================================
-- 6) Address search test — verify Search Optimization is active
-- ============================================================

SELECT
    'DIM_ADDRESS_SEARCH row count'  AS check_name,
    COUNT(*)                        AS value
FROM GOLD.DIM_ADDRESS_SEARCH

UNION ALL

SELECT
    'Search optimisation events (last hour)',
    COUNT(*)
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    TABLE_NAME       => 'DIM_ADDRESS_SEARCH'
));

-- ============================================================
-- 7) Nightly task status
-- ============================================================

SELECT
    NAME,
    STATE,
    SCHEDULE,
    LAST_COMMITTED_ON,
    COMMENT
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    TASK_NAME => 'REFRESH_DIM_ADDRESS_SEARCH'
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;
