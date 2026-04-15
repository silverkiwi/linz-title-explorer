-- ============================================================
-- 15_gold_layer.sql
-- GOLD Layer: views and reporting for LINZ Landonline data
-- Designed to work with currently loaded data and auto-enrich
-- as missing tables (ESTATE_SHARE, PROPRIETOR, PARCEL, etc.)
-- become available via LEFT JOINs.
-- ============================================================

USE DATABASE L;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- SECTION 1: Core Dimension Views
-- ============================================================

-- 1a. DIM_TITLE — enriched title with land district lookup
--     (LAND_DISTRICT NULLs until that table is loaded)
CREATE OR REPLACE VIEW GOLD.DIM_TITLE AS
SELECT
    t.TITLE_NO,
    t.LDT_LOC_ID,
    ld.NAME                                                            AS LAND_DISTRICT_NAME,
    ld.ABBREV                                                          AS LAND_DISTRICT_ABBREV,
    t.STATUS                                                           AS TITLE_STATUS,
    t.TYPE                                                             AS TITLE_TYPE,
    t.REGISTER_TYPE,
    t.ISSUE_DATE,
    t.GUARANTEE_STATUS,
    t.PROVISIONAL,
    t.MAORI_LAND,
    t.STE_ID,
    t.SUR_WRK_ID,
    t.IS_CURRENT,
    CASE WHEN t.STATUS IN ('LIVE', 'REGD') THEN TRUE ELSE FALSE END    AS IS_ACTIVE,
    t.TTL_TITLE_NO_SRS,
    t.TTL_TITLE_NO_HEAD_SRS,
    t.LOAD_TS
FROM SILVER.TITLE t
LEFT JOIN SILVER.LAND_DISTRICT ld ON t.LDT_LOC_ID = ld.LDT_LOC_ID;

-- 1b. DIM_APPELLATION — parcel appellation enriched
CREATE OR REPLACE VIEW GOLD.DIM_APPELLATION AS
SELECT
    a.ID                   AS APPELLATION_ID,
    a.PAR_ID,
    a.APPELLATION_VALUE,
    a.TYPE                 AS APPELLATION_TYPE,
    a.STATUS,
    a.TITLE_FLAG,
    a.SURVEY_FLAG,
    a.PART_INDICATOR,
    a.PARCEL_TYPE,
    a.PARCEL_VALUE,
    a.SECOND_PARCEL_TYPE,
    a.SECOND_PRCL_VALUE,
    a.BLOCK_NUMBER,
    a.SUB_TYPE,
    a.SUB_TYPE_POSITION,
    a.MAORI_NAME,
    a.HEIGHT_LIMITED,
    a.OTHER_APPELLATION,
    a.LOAD_TS
FROM SILVER.APPELLATION a;

-- ============================================================
-- SECTION 2: Bridge / Association Views
-- ============================================================

-- 2a. BRIDGE_TITLE_PARCEL — title ↔ parcel links
CREATE OR REPLACE VIEW GOLD.BRIDGE_TITLE_PARCEL AS
SELECT
    tpa.TTL_TITLE_NO       AS TITLE_NO,
    tpa.PAR_ID             AS PARCEL_ID,
    tpa.ID,
    tpa.SOURCE,
    tpa.LOAD_TS
FROM SILVER.TITLE_PARCEL_ASSOCIATION tpa;

-- ============================================================
-- SECTION 3: Fact Views
-- ============================================================

-- 3a. FACT_TITLE_ESTATE — estate ownership chain
--     ESTATE_SHARE and PROPRIETOR return NULLs until loaded
CREATE OR REPLACE VIEW GOLD.FACT_TITLE_ESTATE AS
SELECT
    te.ID                  AS TITLE_ESTATE_ID,
    te.TTL_TITLE_NO        AS TITLE_NO,
    te.TYPE                AS ESTATE_TYPE,
    te.STATUS              AS ESTATE_STATUS,
    te.SHARE               AS ESTATE_SHARE_FRACTION,
    te.PURPOSE,
    te.TIMESHARE_WEEK_NO,
    te.TERM,
    te.ORIGINAL_FLAG,
    te.LGD_ID,
    te.IS_CURRENT,
    -- Estate share (loaded separately — NULL until available)
    es.ID                  AS ESTATE_SHARE_ID,
    es.SHARE               AS SHARE_FRACTION,
    es.EXECUTORSHIP,
    es.SHARE_MEMORIAL,
    es.IS_CURRENT          AS SHARE_IS_CURRENT,
    -- Proprietor (loaded separately — NULL until available)
    p.ID                   AS PROPRIETOR_ID,
    p.TYPE                 AS PROPRIETOR_TYPE,
    p.PRIME_SURNAME,
    p.PRIME_OTHER_NAMES,
    p.NAME_SUFFIX,
    p.STATUS               AS PROPRIETOR_STATUS,
    CASE
        WHEN p.TYPE = 'INDV' THEN TRIM(CONCAT_WS(' ', p.PRIME_OTHER_NAMES, p.PRIME_SURNAME, p.NAME_SUFFIX))
        WHEN p.TYPE = 'CORP' THEN p.PRIME_SURNAME   -- corporations store name in surname field
        ELSE NULL
    END                    AS PROPRIETOR_NAME,
    te.LOAD_TS
FROM SILVER.TITLE_ESTATE te
LEFT JOIN SILVER.ESTATE_SHARE es ON te.ID    = es.ETT_ID
LEFT JOIN SILVER.PROPRIETOR   p  ON es.ID    = p.ETS_ID;

-- 3b. FACT_TITLE_INSTRUMENT — registered instruments per title
CREATE OR REPLACE VIEW GOLD.FACT_TITLE_INSTRUMENT AS
SELECT
    tit.TTL_TITLE_NO           AS TITLE_NO,
    ti.ID                      AS TIN_ID,
    ti.INST_NO,
    ti.TRT_GRP,
    ti.TRT_TYPE,
    tt.DESCRIPTION             AS TRANSACTION_TYPE_DESC,
    CONCAT_WS('-', ti.TRT_GRP, ti.TRT_TYPE) AS INSTRUMENT_CODE,
    ti.PRIORITY_NO,
    ti.STATUS                  AS INSTRUMENT_STATUS,
    ti.LODGED_DATETIME,
    ti.TIN_ID_PARENT,
    ti.DLG_ID,
    ti.LDT_LOC_ID,
    ti.IS_CURRENT,
    ti.LOAD_TS
FROM SILVER.TITLE_INSTRUMENT_TITLE tit
JOIN  SILVER.TITLE_INSTRUMENT ti    ON tit.TIN_ID = ti.ID
LEFT JOIN SILVER.TRANSACTION_TYPE tt
    ON ti.TRT_GRP = tt.GRP AND ti.TRT_TYPE = tt.TYPE;

-- 3c. FACT_TITLE_ENCUMBRANCE — encumbrances on titles with beneficiary detail
--     ENCUMBRANCEE returns NULLs until loaded
CREATE OR REPLACE VIEW GOLD.FACT_TITLE_ENCUMBRANCE AS
SELECT
    te.TTL_TITLE_NO            AS TITLE_NO,
    te.ID                      AS TITLE_ENCUMBRANCE_ID,
    te.ENC_ID,
    te.STATUS                  AS TITLE_ENCUMBRANCE_STATUS,
    e.STATUS                   AS ENCUMBRANCE_STATUS,
    e.TERM                     AS ENCUMBRANCE_TERM,
    -- Encumbrancee (beneficiary) — NULL until table loaded
    enc.NAME                   AS ENCUMBRANCEE_NAME,
    enc.STATUS                 AS ENCUMBRANCEE_STATUS,
    -- Encumbrance share detail (linking record, no fraction column)
    es.STATUS                  AS ENCUMBRANCE_SHARE_STATUS,
    te.IS_CURRENT,
    te.LOAD_TS
FROM SILVER.TITLE_ENCUMBRANCE te
LEFT JOIN SILVER.ENCUMBRANCE       e   ON te.ENC_ID  = e.ID
LEFT JOIN SILVER.ENCUMBRANCEE      enc ON enc.ENS_ID = e.ID
LEFT JOIN SILVER.ENCUMBRANCE_SHARE es  ON es.ENC_ID  = e.ID  AND es.STATUS = 'REGD';

-- ============================================================
-- SECTION 4: Navigation / Lineage Views
-- ============================================================

-- 4a. V_TITLE_LINEAGE — predecessor and successor titles
CREATE OR REPLACE VIEW GOLD.V_TITLE_LINEAGE AS
SELECT
    th.ID,
    th.STATUS,
    th.TTL_TITLE_NO_PRIOR      AS PRIOR_TITLE_NO,
    t_prior.STATUS             AS PRIOR_TITLE_STATUS,
    t_prior.TYPE               AS PRIOR_TITLE_TYPE,
    t_prior.ISSUE_DATE         AS PRIOR_ISSUE_DATE,
    th.TTL_TITLE_NO_FLW        AS FOLLOWING_TITLE_NO,
    t_fol.STATUS               AS FOLLOWING_TITLE_STATUS,
    t_fol.TYPE                 AS FOLLOWING_TITLE_TYPE,
    t_fol.ISSUE_DATE           AS FOLLOWING_ISSUE_DATE,
    th.TDR_ID,
    th.LOAD_TS
FROM SILVER.TITLE_HIERARCHY th
LEFT JOIN SILVER.TITLE t_prior ON th.TTL_TITLE_NO_PRIOR = t_prior.TITLE_NO
LEFT JOIN SILVER.TITLE t_fol   ON th.TTL_TITLE_NO_FLW   = t_fol.TITLE_NO;

-- 4b. V_TITLE_APPELLATION — legal appellation per title (via parcel link)
--     Returns empty until TITLE_PARCEL_ASSOCIATION is loaded
CREATE OR REPLACE VIEW GOLD.V_TITLE_APPELLATION AS
SELECT
    tpa.TTL_TITLE_NO           AS TITLE_NO,
    tpa.PAR_ID,
    a.ID                       AS APPELLATION_ID,
    a.APPELLATION_VALUE,
    a.TYPE                     AS APPELLATION_TYPE,
    a.STATUS                   AS APPELLATION_STATUS,
    a.TITLE_FLAG,
    a.PART_INDICATOR,
    a.PARCEL_TYPE,
    a.PARCEL_VALUE,
    a.SECOND_PARCEL_TYPE,
    a.SECOND_PRCL_VALUE,
    a.BLOCK_NUMBER,
    a.SUB_TYPE,
    a.MAORI_NAME,
    a.HEIGHT_LIMITED,
    a.OTHER_APPELLATION
FROM SILVER.TITLE_PARCEL_ASSOCIATION tpa
JOIN SILVER.APPELLATION a
    ON a.PAR_ID = tpa.PAR_ID AND a.TITLE_FLAG = 'Y';

-- 4c. V_TITLE_LEGAL — legal descriptions and their parcels per title
--     Returns empty until LEGAL_DESCRIPTION is loaded
CREATE OR REPLACE VIEW GOLD.V_TITLE_LEGAL AS
SELECT
    ld.TTL_TITLE_NO            AS TITLE_NO,
    ld.ID                      AS LEGAL_DESC_ID,
    ld.TYPE                    AS LEGAL_DESC_TYPE,
    ld.STATUS                  AS LEGAL_DESC_STATUS,
    ld.TOTAL_AREA,
    ld.LEGAL_DESC_TEXT,
    ldp.PAR_ID,
    ldp.SEQUENCE               AS PARCEL_SEQUENCE,
    ldp.PART_AFFECTED,
    ldp.SHARE                  AS PARCEL_SHARE,
    ldp.SUR_WRK_ID
FROM SILVER.LEGAL_DESCRIPTION ld
LEFT JOIN SILVER.LEGAL_DESCRIPTION_PARCEL ldp ON ldp.LGD_ID = ld.ID;

-- ============================================================
-- SECTION 5: Address Integration
-- ============================================================

-- 5a. Rebuild DIM_ADDRESS with robust component pivot and title join
CREATE OR REPLACE VIEW GOLD.DIM_ADDRESS AS
WITH current_components AS (
    SELECT
        address_id,
        component_type,
        component_value
    FROM SILVER.AIMS_ADDRESS_COMPONENT
    WHERE end_lifecycle IS NULL
       OR end_lifecycle IN ('', '00000000', '0000-00-00')
),
pivoted AS (
    SELECT
        address_id,
        MAX(CASE WHEN component_type = 'Unit Type'             THEN component_value END) AS unit_type,
        MAX(CASE WHEN component_type = 'Unit Value'            THEN component_value END) AS unit_value,
        MAX(CASE WHEN component_type = 'Address Number'        THEN component_value END) AS street_number,
        MAX(CASE WHEN component_type = 'Address Number High'   THEN component_value END) AS street_number_high,
        MAX(CASE WHEN component_type = 'Address Number Suffix' THEN component_value END) AS street_number_suffix,
        MAX(CASE WHEN component_type = 'Road Name'             THEN component_value END) AS road_name,
        MAX(CASE WHEN component_type = 'Road Type Name'        THEN component_value END) AS road_type,
        MAX(CASE WHEN component_type IN ('Road Suffix', 'Road Suffix Name') THEN component_value END) AS road_suffix,
        MAX(CASE WHEN component_type = 'Suburb/Locality Name'  THEN component_value END) AS suburb,
        MAX(CASE WHEN component_type = 'Town/City Name'        THEN component_value END) AS city,
        MAX(CASE WHEN component_type = 'Postcode'              THEN component_value END) AS postcode
    FROM current_components
    GROUP BY address_id
),
-- NOTE: Snowflake CONCAT_WS returns NULL if any argument is NULL (unlike MySQL/PG).
-- Use ARRAY_TO_STRING(ARRAY_COMPACT(...)) to skip NULLs safely.
built AS (
    SELECT
        address_id,
        unit_type, unit_value, street_number, street_number_high, street_number_suffix,
        road_name, road_type, road_suffix, suburb, city, postcode,
        NULLIF(TRIM(ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
            CASE WHEN unit_type IS NOT NULL THEN unit_type || ' ' || COALESCE(unit_value,'') END,
            -- Append suffix directly to number (43 + A → 43A); ranges use slash (43/47)
            CASE WHEN street_number_high   IS NOT NULL THEN street_number || '/' || street_number_high
                 WHEN street_number_suffix IS NOT NULL THEN street_number || street_number_suffix
                 ELSE street_number END,
            road_name,
            road_type,
            road_suffix
        )), ' ')), '') AS street_address
    FROM pivoted
)
SELECT
    a.address_id,
    a.parcel_id,
    tpa.TTL_TITLE_NO                                                         AS title_no,
    b.unit_type,
    b.unit_value,
    b.street_number,
    b.street_number_high,
    b.street_number_suffix,
    b.road_name,
    b.road_type,
    b.suburb,
    b.city,
    b.postcode,
    b.street_address,
    NULLIF(TRIM(ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
        b.street_address, b.suburb, b.city, b.postcode
    )), ', ')), '')                                                           AS full_address,
    a.status,
    a.address_class
FROM SILVER.AIMS_ADDRESS a
JOIN built b ON a.address_id = b.address_id
LEFT JOIN SILVER.TITLE_PARCEL_ASSOCIATION tpa ON a.parcel_id = tpa.PAR_ID
WHERE a.status = 'Current';

-- 5b. V_TITLE_ADDRESS — clean title → address lookup
CREATE OR REPLACE VIEW GOLD.V_TITLE_ADDRESS AS
SELECT DISTINCT
    title_no,
    address_id,
    parcel_id,
    full_address,
    street_address,
    unit_type,
    unit_value,
    street_number,
    street_number_high,
    street_number_suffix,
    road_name,
    road_type,
    suburb,
    city,
    postcode
FROM GOLD.DIM_ADDRESS
WHERE title_no IS NOT NULL;

-- ============================================================
-- SECTION 6: 360° Summary View
-- ============================================================

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
        COUNT(DISTINCT ti.ID)                  AS INSTRUMENT_COUNT,
        MAX(ti.LODGED_DATETIME)                AS LATEST_INSTRUMENT_DATE,
        MAX_BY(ti.INST_NO, ti.LODGED_DATETIME) AS LATEST_INST_NO
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
)
SELECT
    t.TITLE_NO,
    t.TITLE_STATUS,
    t.TITLE_TYPE,
    t.REGISTER_TYPE,
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
LEFT JOIN estate   e  ON t.TITLE_NO = e.TTL_TITLE_NO
LEFT JOIN ownership o ON t.TITLE_NO = o.TTL_TITLE_NO
LEFT JOIN instr    i  ON t.TITLE_NO = i.TTL_TITLE_NO
LEFT JOIN enc      n  ON t.TITLE_NO = n.TTL_TITLE_NO
LEFT JOIN par      p  ON t.TITLE_NO = p.TTL_TITLE_NO
LEFT JOIN addr     ad ON t.TITLE_NO = ad.title_no
LEFT JOIN appel    ap ON t.TITLE_NO = ap.TITLE_NO
LEFT JOIN lineage  ln ON t.TITLE_NO = ln.TITLE_NO;

-- ============================================================
-- SECTION 7: Reporting Views
-- ============================================================

-- 7a. Instrument activity by month and type (trend analysis)
CREATE OR REPLACE VIEW GOLD.RPT_INSTRUMENT_ACTIVITY AS
SELECT
    DATE_TRUNC('month', ti.LODGED_DATETIME) AS LODGED_MONTH,
    ti.TRT_GRP,
    ti.TRT_TYPE,
    tt.DESCRIPTION                          AS TRANSACTION_TYPE_DESC,
    COUNT(*)                                AS INSTRUMENT_COUNT,
    COUNT(DISTINCT tit.TTL_TITLE_NO)        AS TITLES_AFFECTED
FROM SILVER.TITLE_INSTRUMENT ti
JOIN SILVER.TITLE_INSTRUMENT_TITLE tit   ON tit.TIN_ID = ti.ID
LEFT JOIN SILVER.TRANSACTION_TYPE tt
    ON ti.TRT_GRP = tt.GRP AND ti.TRT_TYPE = tt.TYPE
WHERE ti.LODGED_DATETIME IS NOT NULL
GROUP BY 1, 2, 3, 4;

-- 7b. Active encumbrances with beneficiary details
CREATE OR REPLACE VIEW GOLD.RPT_ACTIVE_ENCUMBRANCES AS
SELECT
    te.TTL_TITLE_NO            AS TITLE_NO,
    t.STATUS                   AS TITLE_STATUS,
    t.LDT_LOC_ID,
    te.ID                      AS TITLE_ENCUMBRANCE_ID,
    te.ENC_ID,
    e.TERM                     AS ENCUMBRANCE_TERM,
    enc.NAME                   AS ENCUMBRANCEE_NAME,
    enc.STATUS                 AS ENCUMBRANCEE_STATUS
FROM SILVER.TITLE_ENCUMBRANCE te
JOIN  SILVER.TITLE       t   ON te.TTL_TITLE_NO = t.TITLE_NO
JOIN  SILVER.ENCUMBRANCE e   ON te.ENC_ID        = e.ID
LEFT JOIN SILVER.ENCUMBRANCEE enc ON enc.ENS_ID  = e.ID
WHERE te.IS_CURRENT = TRUE
  AND t.IS_CURRENT  = TRUE;

-- 7c. Estate type distribution per land district
CREATE OR REPLACE VIEW GOLD.RPT_ESTATE_BY_LAND_DISTRICT AS
SELECT
    t.LDT_LOC_ID,
    ld.NAME                    AS LAND_DISTRICT,
    te.TYPE                    AS ESTATE_TYPE,
    COUNT(*)                   AS TITLE_COUNT
FROM SILVER.TITLE_ESTATE te
JOIN  SILVER.TITLE t           ON te.TTL_TITLE_NO = t.TITLE_NO
LEFT JOIN SILVER.LAND_DISTRICT ld ON t.LDT_LOC_ID = ld.LDT_LOC_ID
WHERE te.IS_CURRENT = TRUE
GROUP BY 1, 2, 3;

-- 7d. Instrument status breakdown by transaction group
CREATE OR REPLACE VIEW GOLD.RPT_INSTRUMENT_STATUS AS
SELECT
    ti.TRT_GRP,
    ti.TRT_TYPE,
    tt.DESCRIPTION             AS TRANSACTION_TYPE_DESC,
    ti.STATUS                  AS INSTRUMENT_STATUS,
    COUNT(*)                   AS CNT
FROM SILVER.TITLE_INSTRUMENT ti
LEFT JOIN SILVER.TRANSACTION_TYPE tt
    ON ti.TRT_GRP = tt.GRP AND ti.TRT_TYPE = tt.TYPE
GROUP BY 1, 2, 3, 4;

-- 7e. Title lineage depth — how many steps from root titles
CREATE OR REPLACE VIEW GOLD.RPT_TITLE_LINEAGE_DEPTH AS
WITH RECURSIVE lineage_chain AS (
    -- Anchor: titles with no predecessor (root titles)
    SELECT
        t.TITLE_NO,
        t.TITLE_NO              AS ROOT_TITLE_NO,
        0                       AS DEPTH
    FROM SILVER.TITLE t
    WHERE NOT EXISTS (
        SELECT 1 FROM SILVER.TITLE_HIERARCHY th
        WHERE th.TTL_TITLE_NO_FLW = t.TITLE_NO AND th.STATUS = 'CURR'
    )
    UNION ALL
    -- Recursive: follow the chain forward
    SELECT
        th.TTL_TITLE_NO_FLW     AS TITLE_NO,
        lc.ROOT_TITLE_NO,
        lc.DEPTH + 1            AS DEPTH
    FROM lineage_chain lc
    JOIN SILVER.TITLE_HIERARCHY th
        ON th.TTL_TITLE_NO_PRIOR = lc.TITLE_NO AND th.STATUS = 'CURR'
)
SELECT
    TITLE_NO,
    ROOT_TITLE_NO,
    DEPTH
FROM lineage_chain;

-- ============================================================
-- SECTION 8: Smoke check — validate views return data
-- ============================================================

SELECT
    'DIM_TITLE'                AS view_name,  COUNT(*) AS row_count FROM GOLD.DIM_TITLE
UNION ALL SELECT 'DIM_APPELLATION',            COUNT(*) FROM GOLD.DIM_APPELLATION
UNION ALL SELECT 'BRIDGE_TITLE_PARCEL',        COUNT(*) FROM GOLD.BRIDGE_TITLE_PARCEL
UNION ALL SELECT 'FACT_TITLE_ESTATE',          COUNT(*) FROM GOLD.FACT_TITLE_ESTATE
UNION ALL SELECT 'FACT_TITLE_INSTRUMENT',      COUNT(*) FROM GOLD.FACT_TITLE_INSTRUMENT
UNION ALL SELECT 'FACT_TITLE_ENCUMBRANCE',     COUNT(*) FROM GOLD.FACT_TITLE_ENCUMBRANCE
UNION ALL SELECT 'V_TITLE_LINEAGE',            COUNT(*) FROM GOLD.V_TITLE_LINEAGE
UNION ALL SELECT 'V_TITLE_APPELLATION',        COUNT(*) FROM GOLD.V_TITLE_APPELLATION
UNION ALL SELECT 'V_TITLE_LEGAL',              COUNT(*) FROM GOLD.V_TITLE_LEGAL
UNION ALL SELECT 'DIM_ADDRESS',                COUNT(*) FROM GOLD.DIM_ADDRESS
UNION ALL SELECT 'V_TITLE_ADDRESS',            COUNT(*) FROM GOLD.V_TITLE_ADDRESS
UNION ALL SELECT 'RPT_INSTRUMENT_ACTIVITY',    COUNT(*) FROM GOLD.RPT_INSTRUMENT_ACTIVITY
UNION ALL SELECT 'RPT_ACTIVE_ENCUMBRANCES',    COUNT(*) FROM GOLD.RPT_ACTIVE_ENCUMBRANCES
UNION ALL SELECT 'RPT_ESTATE_BY_LAND_DISTRICT',COUNT(*) FROM GOLD.RPT_ESTATE_BY_LAND_DISTRICT
UNION ALL SELECT 'RPT_INSTRUMENT_STATUS',      COUNT(*) FROM GOLD.RPT_INSTRUMENT_STATUS
ORDER BY view_name;
