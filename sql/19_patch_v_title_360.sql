-- ============================================================
-- 19_patch_v_title_360.sql
-- Targeted patch: replace V_TITLE_360 only.
--
-- Use this when 15_gold_layer.sql fails partway through (e.g.
-- because SILVER.ENCUMBRANCE_SHARE doesn't exist yet) and
-- V_TITLE_360 never gets updated as a result.
--
-- Run via:
--   snowsql -c linz -f sql/19_patch_v_title_360.sql
-- ============================================================

USE DATABASE L;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE VIEW GOLD.V_TITLE_360 AS
WITH estate AS (
    SELECT
        TTL_TITLE_NO,
        COUNT(*)                AS ESTATE_COUNT,
        COUNT_IF(IS_CURRENT)    AS CURRENT_ESTATE_COUNT,
        LISTAGG(DISTINCT TYPE, ', ') WITHIN GROUP (ORDER BY TYPE) AS ESTATE_TYPES
    FROM SILVER.TITLE_ESTATE
    GROUP BY TTL_TITLE_NO
),
ownership AS (
    -- Populated once ESTATE_SHARE + PROPRIETOR are loaded
    SELECT
        te.TTL_TITLE_NO,
        COUNT(DISTINCT p.ID)    AS PROPRIETOR_COUNT,
        LISTAGG(
            CASE
                WHEN p.TYPE = 'INDV' THEN TRIM(CONCAT_WS(' ', p.PRIME_OTHER_NAMES, p.PRIME_SURNAME))
                WHEN p.TYPE = 'CORP' THEN p.PRIME_SURNAME
                ELSE NULL
            END, ' | '
        ) WITHIN GROUP (ORDER BY p.PRIME_SURNAME) AS PROPRIETORS
    FROM SILVER.TITLE_ESTATE te
    JOIN SILVER.ESTATE_SHARE es ON te.ID    = es.ETT_ID
    JOIN SILVER.PROPRIETOR   p  ON es.ID    = p.ETS_ID
    WHERE p.STATUS <> 'HIST'
    GROUP BY te.TTL_TITLE_NO
),
instr AS (
    SELECT
        tit.TTL_TITLE_NO,
        COUNT(DISTINCT ti.ID)                   AS INSTRUMENT_COUNT,
        MAX(ti.LODGED_DATETIME)                 AS LATEST_INSTRUMENT_DATE,
        MAX_BY(ti.INST_NO,  ti.LODGED_DATETIME) AS LATEST_INST_NO,
        MAX_BY(ti.TRT_GRP,  ti.LODGED_DATETIME) AS LATEST_TRT_GRP,
        MAX_BY(ti.TRT_TYPE, ti.LODGED_DATETIME) AS LATEST_TRT_TYPE
    FROM SILVER.TITLE_INSTRUMENT_TITLE tit
    JOIN SILVER.TITLE_INSTRUMENT ti ON tit.TIN_ID = ti.ID
    GROUP BY tit.TTL_TITLE_NO
),
enc AS (
    SELECT
        TTL_TITLE_NO,
        COUNT(*)                AS ENCUMBRANCE_COUNT,
        COUNT_IF(IS_CURRENT)    AS CURRENT_ENCUMBRANCE_COUNT
    FROM SILVER.TITLE_ENCUMBRANCE
    GROUP BY TTL_TITLE_NO
),
par AS (
    -- Populated once TITLE_PARCEL_ASSOCIATION loads
    SELECT
        TTL_TITLE_NO,
        COUNT(DISTINCT PAR_ID)  AS PARCEL_COUNT
    FROM SILVER.TITLE_PARCEL_ASSOCIATION
    GROUP BY TTL_TITLE_NO
),
addr AS (
    -- Populated once TPA loads and links addresses to titles
    SELECT
        title_no,
        MIN(full_address)       AS PRIMARY_ADDRESS
    FROM GOLD.V_TITLE_ADDRESS
    GROUP BY title_no
),
appel AS (
    -- Populated once TPA + APPELLATION both loaded
    SELECT
        TITLE_NO,
        LISTAGG(DISTINCT APPELLATION_VALUE, ' | ') WITHIN GROUP (ORDER BY APPELLATION_VALUE) AS APPELLATIONS
    FROM GOLD.V_TITLE_APPELLATION
    WHERE APPELLATION_STATUS = 'CURR'
    GROUP BY TITLE_NO
),
lineage AS (
    -- Most recent predecessor title
    SELECT
        TTL_TITLE_NO_FLW        AS TITLE_NO,
        TTL_TITLE_NO_PRIOR      AS PRIOR_TITLE_NO
    FROM SILVER.TITLE_HIERARCHY
    WHERE STATUS = 'CURR'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TTL_TITLE_NO_FLW ORDER BY ID DESC) = 1
),
latest_tt AS (
    -- Human-readable type + description for the most recently lodged instrument
    SELECT
        i.TTL_TITLE_NO,
        CONCAT_WS('-', i.LATEST_TRT_GRP, i.LATEST_TRT_TYPE)  AS LATEST_INST_TYPE,
        tt.DESCRIPTION                                         AS LATEST_INST_TYPE_DESC
    FROM instr i
    LEFT JOIN SILVER.TRANSACTION_TYPE tt
        ON  i.LATEST_TRT_GRP  = tt.GRP
        AND i.LATEST_TRT_TYPE = tt.TYPE
)
SELECT
    t.TITLE_NO,
    t.TITLE_STATUS,
    CASE t.TITLE_STATUS
        WHEN 'LIVE' THEN 'Live'
        WHEN 'HIST' THEN 'Historical'
        WHEN 'CANC' THEN 'Cancelled'
        ELSE t.TITLE_STATUS
    END                                             AS TITLE_STATUS_DESC,
    t.TITLE_TYPE,
    CASE t.TITLE_TYPE
        WHEN 'F'   THEN 'Fee Simple'
        WHEN 'LH'  THEN 'Leasehold'
        WHEN 'SRS' THEN 'Unit Title (SRS)'
        WHEN 'ML'  THEN 'Maori Land'
        WHEN 'MOT' THEN 'Miscellaneous Owners'
        ELSE t.TITLE_TYPE
    END                                             AS TITLE_TYPE_DESC,
    t.REGISTER_TYPE,
    CASE t.REGISTER_TYPE
        WHEN 'CF'  THEN 'Computer Freehold Register'
        WHEN 'CI'  THEN 'Computer Interest Register'
        ELSE t.REGISTER_TYPE
    END                                             AS REGISTER_TYPE_DESC,
    t.ISSUE_DATE,
    t.IS_CURRENT,
    t.IS_ACTIVE,
    t.LAND_DISTRICT_NAME,
    t.LDT_LOC_ID,
    t.MAORI_LAND,
    t.GUARANTEE_STATUS,
    -- Ownership (NULLs until ESTATE_SHARE + PROPRIETOR load)
    COALESCE(o.PROPRIETOR_COUNT,           0)  AS PROPRIETOR_COUNT,
    o.PROPRIETORS,
    -- Estates
    COALESCE(e.ESTATE_COUNT,               0)  AS ESTATE_COUNT,
    COALESCE(e.CURRENT_ESTATE_COUNT,       0)  AS CURRENT_ESTATE_COUNT,
    e.ESTATE_TYPES,
    -- Instruments
    COALESCE(i.INSTRUMENT_COUNT,           0)  AS INSTRUMENT_COUNT,
    i.LATEST_INSTRUMENT_DATE,
    i.LATEST_INST_NO,
    lt.LATEST_INST_TYPE,
    lt.LATEST_INST_TYPE_DESC,
    -- Encumbrances
    COALESCE(n.ENCUMBRANCE_COUNT,          0)  AS ENCUMBRANCE_COUNT,
    COALESCE(n.CURRENT_ENCUMBRANCE_COUNT,  0)  AS CURRENT_ENCUMBRANCE_COUNT,
    -- Parcels (0 until TPA loads)
    COALESCE(p.PARCEL_COUNT,               0)  AS PARCEL_COUNT,
    -- Address (NULL until TPA loads)
    ad.PRIMARY_ADDRESS,
    -- Appellation / legal description (NULL until TPA + APPELLATION loaded)
    ap.APPELLATIONS,
    -- Title lineage
    ln.PRIOR_TITLE_NO,
    CURRENT_TIMESTAMP()                        AS REFRESHED_AT
FROM GOLD.DIM_TITLE t
LEFT JOIN estate    e  ON t.TITLE_NO = e.TTL_TITLE_NO
LEFT JOIN ownership o  ON t.TITLE_NO = o.TTL_TITLE_NO
LEFT JOIN instr     i  ON t.TITLE_NO = i.TTL_TITLE_NO
LEFT JOIN latest_tt lt ON t.TITLE_NO = lt.TTL_TITLE_NO
LEFT JOIN enc       n  ON t.TITLE_NO = n.TTL_TITLE_NO
LEFT JOIN par       p  ON t.TITLE_NO = p.TTL_TITLE_NO
LEFT JOIN addr      ad ON t.TITLE_NO = ad.title_no
LEFT JOIN appel     ap ON t.TITLE_NO = ap.TITLE_NO
LEFT JOIN lineage   ln ON t.TITLE_NO = ln.TITLE_NO;

-- ============================================================
-- Validation: confirm the new columns are present and the
-- view resolves without errors at runtime.
-- ============================================================

SELECT
    CASE
        WHEN COUNT_IF(COLUMN_NAME = 'TITLE_STATUS_DESC')    = 0 THEN 'MISSING: TITLE_STATUS_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'TITLE_TYPE_DESC')      = 0 THEN 'MISSING: TITLE_TYPE_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'REGISTER_TYPE_DESC')   = 0 THEN 'MISSING: REGISTER_TYPE_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'LATEST_INST_TYPE')     = 0 THEN 'MISSING: LATEST_INST_TYPE'
        WHEN COUNT_IF(COLUMN_NAME = 'LATEST_INST_TYPE_DESC')= 0 THEN 'MISSING: LATEST_INST_TYPE_DESC'
        WHEN COUNT_IF(COLUMN_NAME = 'PRIMARY_ADDRESS')      = 0 THEN 'MISSING: PRIMARY_ADDRESS'
        WHEN COUNT_IF(COLUMN_NAME = 'APPELLATIONS')         = 0 THEN 'MISSING: APPELLATIONS'
        WHEN COUNT_IF(COLUMN_NAME = 'PRIOR_TITLE_NO')       = 0 THEN 'MISSING: PRIOR_TITLE_NO'
        ELSE 'OK — all required V_TITLE_360 columns present'
    END AS column_check
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'GOLD'
  AND TABLE_NAME   = 'V_TITLE_360';

-- Runtime test: fetch 3 rows — will fail if any column ref is bad
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
LIMIT 3;
