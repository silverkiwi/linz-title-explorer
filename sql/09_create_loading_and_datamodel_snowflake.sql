-- LINZ Snowflake loading + core data model
-- Based on lds-expanded-final.md v3.1 (July 2024)
-- Domains: Title, Parcel, Instrument, Estate, Encumbrance, Legal Description

USE DATABASE L;
USE SCHEMA PUBLIC;

-- ============================================================
-- 1) Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS OPS;

-- ============================================================
-- 2) Stage observability + load control
-- ============================================================
CREATE TABLE IF NOT EXISTS OPS.STAGE_FILE_INVENTORY (
  RELATIVE_PATH    STRING,
  SIZE             NUMBER,
  MD5              STRING,
  LAST_MODIFIED    TIMESTAMP_TZ,
  SNAPSHOT_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OPS.FILE_LOAD_CONTROL (
  LOAD_ID            NUMBER AUTOINCREMENT,
  SOURCE_SYSTEM      STRING         DEFAULT 'LINZ',
  SOURCE_ENTITY      STRING,
  STAGE_NAME         STRING         DEFAULT '@L.PUBLIC.LINZ_STAGE',
  FILE_NAME          STRING,
  FILE_SIZE          NUMBER,
  FILE_MD5           STRING,
  FILE_LAST_MODIFIED TIMESTAMP_NTZ,
  LOAD_STATUS        STRING         DEFAULT 'DISCOVERED', -- DISCOVERED | LOADED | FAILED | SKIPPED
  ROWS_LOADED        NUMBER,
  ROWS_REJECTED      NUMBER,
  ERROR_MESSAGE      STRING,
  DISCOVERED_AT      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  LOADED_AT          TIMESTAMP_NTZ,
  CONSTRAINT PK_FILE_LOAD_CONTROL PRIMARY KEY (LOAD_ID)
);

-- Capture current stage listing into inventory
LIST @L.PUBLIC.LINZ_STAGE;

INSERT INTO OPS.STAGE_FILE_INVENTORY (RELATIVE_PATH, SIZE, MD5, LAST_MODIFIED)
SELECT
  "name"::STRING,
  "size"::NUMBER,
  "md5"::STRING,
  TRY_TO_TIMESTAMP_TZ("last_modified")
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

CREATE OR REPLACE VIEW OPS.V_LINZ_STAGE_FILES AS
SELECT RELATIVE_PATH, SIZE, MD5, LAST_MODIFIED, SNAPSHOT_AT
FROM OPS.STAGE_FILE_INVENTORY
QUALIFY ROW_NUMBER() OVER (PARTITION BY RELATIVE_PATH, MD5 ORDER BY SNAPSHOT_AT DESC) = 1;

-- Snapshot currently staged files into control table (idempotent by filename+md5)
MERGE INTO OPS.FILE_LOAD_CONTROL t
USING (
  SELECT
    REGEXP_SUBSTR(RELATIVE_PATH, '[^/]+$')  AS FILE_NAME,
    SIZE                                     AS FILE_SIZE,
    MD5                                      AS FILE_MD5,
    LAST_MODIFIED::TIMESTAMP_NTZ             AS FILE_LAST_MODIFIED
  FROM OPS.V_LINZ_STAGE_FILES
) s
ON  t.FILE_NAME = s.FILE_NAME
AND COALESCE(t.FILE_MD5, '') = COALESCE(s.FILE_MD5, '')
WHEN NOT MATCHED THEN INSERT (
  SOURCE_ENTITY, FILE_NAME, FILE_SIZE, FILE_MD5, FILE_LAST_MODIFIED
) VALUES (
  SPLIT_PART(s.FILE_NAME, '-', 3), s.FILE_NAME, s.FILE_SIZE, s.FILE_MD5, s.FILE_LAST_MODIFIED
);

-- ============================================================
-- 3) BRONZE — generic raw landing
-- ============================================================
CREATE TABLE IF NOT EXISTS BRONZE.RAW_LINZ_RECORDS (
  SOURCE_ENTITY      STRING,
  SOURCE_FILE        STRING,
  SOURCE_ROW_NUMBER  NUMBER,
  INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PAYLOAD            VARIANT,
  CONSTRAINT PK_RAW_LINZ_RECORDS PRIMARY KEY (SOURCE_ENTITY, SOURCE_FILE, SOURCE_ROW_NUMBER)
);

-- ============================================================
-- 4) SILVER — normalised model (aligned to dict v3.1)
-- ============================================================

-- 4.25 Land District
CREATE TABLE IF NOT EXISTS SILVER.LAND_DISTRICT (
  LDT_LOC_ID  NUMBER,        -- LOC_ID in internal schema; export uses LDT_ prefix
  OFF_CODE    STRING,        -- Office code (4-char)
  NAME        STRING,        -- Human-readable name (from export)
  ABBREV      STRING,        -- Abbreviation (from export)
  STATUS      STRING,
  DEFAULT_IND CHAR(1),       -- Y/N default district flag
  USR_TM_ID   STRING,        -- User team ID
  AUDIT_ID    NUMBER,
  SHAPE       GEOGRAPHY,
  LOAD_TS     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_LAND_DISTRICT PRIMARY KEY (LDT_LOC_ID)
);

-- 4.73 Title
CREATE TABLE IF NOT EXISTS SILVER.TITLE (
  TITLE_NO             STRING,
  LDT_LOC_ID           NUMBER,
  STATUS               STRING,        -- Ref: TTLS code group
  ISSUE_DATE           TIMESTAMP_NTZ,
  REGISTER_TYPE        STRING,        -- Ref: TTLR code group
  TYPE                 STRING,        -- Ref: TTLT code group
  AUDIT_ID             NUMBER,
  STE_ID               NUMBER,
  GUARANTEE_STATUS     STRING,        -- Ref: TTLG code group
  PROVISIONAL          CHAR(1),       -- Y/N
  SUR_WRK_ID           NUMBER,
  MAORI_LAND           CHAR(1),       -- Y/null
  TTL_TITLE_NO_SRS     STRING,
  TTL_TITLE_NO_HEAD_SRS STRING,
  IS_CURRENT           BOOLEAN,
  LOAD_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TITLE PRIMARY KEY (TITLE_NO)
);

-- 4.77 Title Estate
CREATE TABLE IF NOT EXISTS SILVER.TITLE_ESTATE (
  ID               NUMBER,
  TTL_TITLE_NO     STRING,
  TYPE             STRING,        -- Ref: ETTT code group
  STATUS           STRING,        -- Ref: TSDS code group
  SHARE            STRING,
  PURPOSE          STRING,
  TIMESHARE_WEEK_NO STRING,
  LGD_ID           NUMBER,
  ACT_TIN_ID_CRT   NUMBER,
  ORIGINAL_FLAG    CHAR(1),       -- Y/N
  TIN_ID_ORIG      NUMBER,
  TERM             STRING,
  ACT_ID_CRT       NUMBER,
  IS_CURRENT       BOOLEAN,
  LOAD_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TITLE_ESTATE PRIMARY KEY (ID)
);

-- 4.22 Estate Share  (bridge between TITLE_ESTATE and PROPRIETOR)
-- Each estate share represents a fractional ownership within a title estate.
CREATE TABLE IF NOT EXISTS SILVER.ESTATE_SHARE (
  ID              NUMBER,
  ETT_ID          NUMBER,        -- FK → TITLE_ESTATE.ID
  STATUS          STRING,        -- Ref: TSDS code group
  SHARE           STRING,        -- e.g. "1/2"
  ACT_TIN_ID_CRT  NUMBER,
  ORIGINAL_FLAG   CHAR(1),       -- Y/N
  SYSTEM_CRT      CHAR(1),       -- Y/N copy-down creation flag
  EXECUTORSHIP    STRING,        -- Ref: ETSE code group
  ACT_ID_CRT      NUMBER,
  SHARE_MEMORIAL  STRING,
  IS_CURRENT      BOOLEAN,
  LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_ESTATE_SHARE PRIMARY KEY (ID)
);

-- 4.55 Proprietor  (links to ESTATE_SHARE, not directly to TITLE_ESTATE)
CREATE TABLE IF NOT EXISTS SILVER.PROPRIETOR (
  ID               NUMBER,
  ETS_ID           NUMBER,        -- FK → ESTATE_SHARE.ID
  STATUS           STRING,        -- Ref: TSDS code group
  TYPE             STRING,        -- Ref: PRPT code group (INDV / CORP)
  PRIME_SURNAME    STRING,
  PRIME_OTHER_NAMES STRING,
  NAME_SUFFIX      STRING,        -- Ref: NMSF code group
  ORIGINAL_FLAG    CHAR(1),       -- Y/N
  LOAD_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_PROPRIETOR PRIMARY KEY (ID)
);

-- 4.79 Title Instrument
CREATE TABLE IF NOT EXISTS SILVER.TITLE_INSTRUMENT (
  ID               NUMBER,
  DLG_ID           NUMBER,
  INST_NO          STRING,
  PRIORITY_NO      NUMBER,
  LDT_LOC_ID       NUMBER,
  LODGED_DATETIME  TIMESTAMP_NTZ,
  STATUS           STRING,        -- Ref: TINS code group
  TRT_GRP          STRING,
  TRT_TYPE         STRING,
  AUDIT_ID         NUMBER,
  TIN_ID_PARENT    NUMBER,
  IS_CURRENT       BOOLEAN,
  LOAD_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TITLE_INSTRUMENT PRIMARY KEY (ID)
);

-- 4.80 Title Instrument Title  (junction: instrument ↔ title)
CREATE TABLE IF NOT EXISTS SILVER.TITLE_INSTRUMENT_TITLE (
  TIN_ID       NUMBER,
  TTL_TITLE_NO STRING,
  AUDIT_ID     NUMBER,
  LOAD_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TITLE_INSTRUMENT_TITLE PRIMARY KEY (TIN_ID, TTL_TITLE_NO)
);

-- 4.84 Transaction Type
CREATE TABLE IF NOT EXISTS SILVER.TRANSACTION_TYPE (
  GRP         STRING,
  TYPE        STRING,
  DESCRIPTION STRING,
  AUDIT_ID    NUMBER,
  LOAD_TS     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TRANSACTION_TYPE PRIMARY KEY (GRP, TYPE)
);

-- 4.19 Encumbrance
CREATE TABLE IF NOT EXISTS SILVER.ENCUMBRANCE (
  ID              NUMBER,
  STATUS          STRING,        -- Ref: TSDS code group
  ACT_TIN_ID_CRT  NUMBER,
  ACT_TIN_ID_ORIG NUMBER,
  ACT_ID_CRT      NUMBER,
  ACT_ID_ORIG     NUMBER,
  TERM            STRING,
  IS_CURRENT      BOOLEAN,
  LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_ENCUMBRANCE PRIMARY KEY (ID)
);

-- 4.21 Encumbrancee  (party that benefits from the encumbrance)
CREATE TABLE IF NOT EXISTS SILVER.ENCUMBRANCEE (
  ID      NUMBER,
  ENS_ID  NUMBER,        -- FK → ENCUMBRANCE_SHARE.ID (or ENCUMBRANCE.ID in simple cases)
  STATUS  STRING,        -- Ref: TSDS code group
  NAME    STRING,
  LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_ENCUMBRANCEE PRIMARY KEY (ID)
);

-- Encumbrance Share  (interest held per encumbrancee in an encumbrance)
CREATE TABLE IF NOT EXISTS SILVER.ENCUMBRANCE_SHARE (
  ID                  NUMBER,
  ENC_ID              NUMBER,        -- FK → ENCUMBRANCE.ID
  STATUS              STRING,        -- Ref: TSDS code group
  ACT_TIN_ID_CRT      NUMBER,
  ACT_ID_CRT          NUMBER,
  ACT_ID_EXT          NUMBER,
  ACT_TIN_ID_EXT      NUMBER,
  SYSTEM_CREATED      CHAR(1),       -- Y/N: created by system copy-down
  SYSTEM_EXTINGUISHED CHAR(1),       -- Y/N: extinguished by system
  IS_CURRENT          BOOLEAN,
  LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_ENCUMBRANCE_SHARE PRIMARY KEY (ID)
);

-- 4.76 Title Encumbrance  (junction: encumbrance ↔ title)
CREATE TABLE IF NOT EXISTS SILVER.TITLE_ENCUMBRANCE (
  ID              NUMBER,
  TTL_TITLE_NO    STRING,
  ENC_ID          NUMBER,
  STATUS          STRING,
  ACT_TIN_ID_CRT  NUMBER,
  ACT_ID_CRT      NUMBER,
  IS_CURRENT      BOOLEAN,
  LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TITLE_ENCUMBRANCE PRIMARY KEY (ID)
);

-- 4.78 Title Hierarchy
CREATE TABLE IF NOT EXISTS SILVER.TITLE_HIERARCHY (
  ID               NUMBER,
  STATUS           STRING,
  TTL_TITLE_NO_PRIOR STRING,
  TTL_TITLE_NO_FLW   STRING,
  TDR_ID           NUMBER,
  ACT_TIN_ID_CRT   NUMBER,
  ACT_ID_CRT       NUMBER,
  LOAD_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TITLE_HIERARCHY PRIMARY KEY (ID)
);

-- 4.49 Parcel
CREATE TABLE IF NOT EXISTS SILVER.PARCEL (
  ID               NUMBER,
  LDT_LOC_ID       NUMBER,
  IMG_ID           NUMBER,
  FEN_ID           NUMBER,
  TOC_CODE         STRING,
  ALT_ID           NUMBER,
  AREA             NUMBER(20,4),
  NONSURVEY_DEF    STRING,
  APPELLATION_DATE TIMESTAMP_NTZ,
  PARCEL_INTENT    STRING,        -- Ref: PARI code group
  STATUS           STRING,        -- Ref: PARS code group
  TOTAL_AREA       NUMBER(20,4),
  CALCULATED_AREA  NUMBER(20,4),
  SE_ROW_ID        NUMBER,
  AUDIT_ID         NUMBER,
  SHAPE            GEOGRAPHY,
  LOAD_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_PARCEL PRIMARY KEY (ID)
);

-- 4.10 Appellation  (expanded to full dict columns)
CREATE TABLE IF NOT EXISTS SILVER.APPELLATION (
  ID                  NUMBER,
  PAR_ID              NUMBER,        -- FK → PARCEL.ID
  TYPE                STRING,        -- Ref: APPT code group
  TITLE_FLAG          CHAR(1),       -- Y/N: appears on title
  SURVEY_FLAG         CHAR(1),       -- Y/N: appears on survey
  STATUS              STRING,        -- Ref: APPS code group
  PART_INDICATOR      STRING,        -- Ref: APPI code group
  MAORI_NAME          STRING,
  SUB_TYPE            STRING,        -- Ref: ASAU code group
  APPELLATION_VALUE   STRING,        -- Primary name/number (60 chars max)
  PARCEL_TYPE         STRING,        -- Ref: ASAP code group
  PARCEL_VALUE        STRING,
  SECOND_PARCEL_TYPE  STRING,        -- Ref: ASAP code group
  SECOND_PRCL_VALUE   STRING,
  BLOCK_NUMBER        STRING,
  SUB_TYPE_POSITION   STRING,        -- Ref: AGNP code group
  ACT_ID_CRT          NUMBER,
  AUDIT_ID            NUMBER,
  HEIGHT_LIMITED      STRING,        -- Height limitation on the parcel
  OTHER_APPELLATION   STRING,        -- Additional appellation text
  LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_APPELLATION PRIMARY KEY (ID)
);

-- 4.26 Legal Description
CREATE TABLE IF NOT EXISTS SILVER.LEGAL_DESCRIPTION (
  ID                    NUMBER,
  TTL_TITLE_NO          STRING,        -- FK → TITLE.TITLE_NO
  TYPE                  STRING,
  STATUS                STRING,
  TOTAL_AREA            NUMBER(20,4),  -- Total area of the legal description
  LEGAL_DESC_TEXT       STRING,        -- Human-readable legal description text
  LOT_NUMBER            STRING,
  DEPOSITED_PLAN_NUMBER STRING,
  FLAT_PLAN_NUMBER      STRING,
  AUDIT_ID              NUMBER,
  LOAD_TS               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_LEGAL_DESCRIPTION PRIMARY KEY (ID)
);

-- 4.27 Legal Description Parcel
CREATE TABLE IF NOT EXISTS SILVER.LEGAL_DESCRIPTION_PARCEL (
  LEG_ID        NUMBER,
  PAR_ID        NUMBER,
  SEQUENCE      NUMBER,               -- Order of parcel within the legal description
  PART_AFFECTED STRING,               -- e.g. 'PART' if only part of parcel affected
  SHARE         STRING,               -- Fractional share (rare)
  SUR_WRK_ID    NUMBER,               -- FK → survey work
  AUDIT_ID      NUMBER,
  LOAD_TS       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_LEGAL_DESCRIPTION_PARCEL PRIMARY KEY (LEG_ID, PAR_ID)
);

-- 4.83 Title Parcel Association
CREATE TABLE IF NOT EXISTS SILVER.TITLE_PARCEL_ASSOCIATION (
  ID           NUMBER,                -- Surrogate key (from LINZ source)
  TTL_TITLE_NO STRING,
  PAR_ID       NUMBER,
  SOURCE       STRING,                -- Origin of the association record
  AUDIT_ID     NUMBER,
  LOAD_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_TITLE_PARCEL_ASSOCIATION PRIMARY KEY (TTL_TITLE_NO, PAR_ID)
);

-- ============================================================
-- 5) GOLD — run 15_gold_layer.sql to create / update all GOLD
--    views and reporting views.  Those definitions are the
--    canonical source of truth and are kept in one place to
--    avoid the duplication and drift that used to live here.
-- ============================================================

-- ============================================================
-- 5b) Schema migrations — add columns that may be absent on
--     databases built from earlier versions of this script.
--     ADD COLUMN IF NOT EXISTS is idempotent; safe to re-run.
-- ============================================================

ALTER TABLE IF EXISTS SILVER.APPELLATION
  ADD COLUMN IF NOT EXISTS HEIGHT_LIMITED    STRING,
  ADD COLUMN IF NOT EXISTS OTHER_APPELLATION STRING;

ALTER TABLE IF EXISTS SILVER.LEGAL_DESCRIPTION
  ADD COLUMN IF NOT EXISTS TTL_TITLE_NO    STRING,
  ADD COLUMN IF NOT EXISTS TOTAL_AREA      NUMBER(20,4),
  ADD COLUMN IF NOT EXISTS LEGAL_DESC_TEXT STRING;

ALTER TABLE IF EXISTS SILVER.LEGAL_DESCRIPTION_PARCEL
  ADD COLUMN IF NOT EXISTS SEQUENCE      NUMBER,
  ADD COLUMN IF NOT EXISTS PART_AFFECTED STRING,
  ADD COLUMN IF NOT EXISTS SHARE         STRING,
  ADD COLUMN IF NOT EXISTS SUR_WRK_ID    NUMBER;

ALTER TABLE IF EXISTS SILVER.TITLE_PARCEL_ASSOCIATION
  ADD COLUMN IF NOT EXISTS ID      NUMBER,
  ADD COLUMN IF NOT EXISTS SOURCE  STRING;
  -- NOTE: STATUS is intentionally absent — LINZ source data for
  -- TITLE_PARCEL_ASSOCIATION does not include a STATUS field.

-- ============================================================
-- 6) Performance clustering
-- ============================================================
ALTER TABLE IF EXISTS SILVER.TITLE               CLUSTER BY (TITLE_NO, STATUS, LDT_LOC_ID);
ALTER TABLE IF EXISTS SILVER.PARCEL              CLUSTER BY (ID, STATUS, LDT_LOC_ID);
ALTER TABLE IF EXISTS SILVER.TITLE_INSTRUMENT    CLUSTER BY (ID, STATUS, LODGED_DATETIME);
ALTER TABLE IF EXISTS SILVER.ESTATE_SHARE        CLUSTER BY (ETT_ID);
ALTER TABLE IF EXISTS SILVER.PROPRIETOR          CLUSTER BY (ETS_ID);
ALTER TABLE IF EXISTS SILVER.TITLE_ENCUMBRANCE   CLUSTER BY (TTL_TITLE_NO, IS_CURRENT);

-- ============================================================
-- 7) Smoke checks
-- ============================================================
SELECT 'STAGE_FILES'        AS CHECK_NAME, COUNT(*) AS ROW_COUNT FROM OPS.V_LINZ_STAGE_FILES
UNION ALL
SELECT 'FILE_LOAD_CONTROL',  COUNT(*) FROM OPS.FILE_LOAD_CONTROL
UNION ALL
SELECT 'SILVER_TABLE_COUNT', COUNT(*)
  FROM INFORMATION_SCHEMA.TABLES
 WHERE TABLE_SCHEMA = 'SILVER'
UNION ALL
SELECT 'GOLD_VIEW_COUNT',    COUNT(*)
  FROM INFORMATION_SCHEMA.VIEWS
 WHERE TABLE_SCHEMA = 'GOLD';
