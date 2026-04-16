-- ============================================================
-- 21_system_codes.sql
-- SILVER.SYSTEM_CODE — centralised code-group lookup table.
--
-- LINZ Landonline publishes a system_code export file that
-- contains all reference codes used across the dataset (title
-- type, register type, guarantee status, estate type, etc.).
-- This script:
--   1) Creates the SILVER.SYSTEM_CODE table.
--   2) Seeds it with the known LINZ code values so the Gold
--      layer views return human-readable descriptions even
--      before a full system_code CSV is ingested.
--
-- The table is keyed on (CODE_GROUP, CODE).  The ingestion
-- pipeline should MERGE into this table from the raw export
-- so that any codes not listed below are still captured.
-- ============================================================

USE DATABASE L;
USE SCHEMA SILVER;
USE WAREHOUSE COMPUTE_WH;

CREATE TABLE IF NOT EXISTS SILVER.SYSTEM_CODE (
    CODE_GROUP   STRING        NOT NULL,   -- e.g. 'TTLT', 'TTLR'
    CODE         STRING        NOT NULL,   -- e.g. 'FHOL', 'FREE'
    DESCRIPTION  STRING,                   -- human-readable label
    AUDIT_ID     NUMBER,
    LOAD_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SYSTEM_CODE PRIMARY KEY (CODE_GROUP, CODE)
);

-- ============================================================
-- Seed: known LINZ code values
-- MERGE so this is safe to re-run without duplicating rows.
-- ============================================================
MERGE INTO SILVER.SYSTEM_CODE t
USING (SELECT * FROM VALUES
    -- ── TTLS  Title Status ──────────────────────────────────
    ('TTLS', 'LIVE', 'Live'),
    ('TTLS', 'HIST', 'Historical'),
    ('TTLS', 'CANC', 'Cancelled'),
    ('TTLS', 'REGD', 'Registered'),

    -- ── TTLT  Title Type ────────────────────────────────────
    -- Modern codes (FHOL-style, used in current exports)
    ('TTLT', 'FHOL', 'Freehold'),
    ('TTLT', 'LEHI', 'Leasehold Interest'),
    ('TTLT', 'LHLH', 'Leasehold'),
    ('TTLT', 'UNIT', 'Unit Title'),
    ('TTLT', 'MORI', 'Māori Land'),
    ('TTLT', 'MOTR', 'Miscellaneous Owners'),
    -- Legacy short codes (older paper-title format)
    ('TTLT', 'F',   'Fee Simple'),
    ('TTLT', 'LH',  'Leasehold'),
    ('TTLT', 'SRS', 'Unit Title (SRS)'),
    ('TTLT', 'ML',  'Māori Land'),
    ('TTLT', 'MOT', 'Miscellaneous Owners'),

    -- ── TTLR  Register Type ─────────────────────────────────
    -- Modern codes
    ('TTLR', 'FREE', 'Freehold Register'),
    ('TTLR', 'LEAS', 'Lease Register'),
    ('TTLR', 'UNIT', 'Unit Register'),
    -- Legacy short codes
    ('TTLR', 'CF',   'Computer Freehold Register'),
    ('TTLR', 'CI',   'Computer Interest Register'),

    -- ── TTLG  Guarantee Status ──────────────────────────────
    ('TTLG', 'GURT', 'Guaranteed'),
    ('TTLG', 'PROV', 'Provisional'),
    ('TTLG', 'LIMI', 'Limited as to Parcels'),
    ('TTLG', 'NONE', 'Not Guaranteed'),
    ('TTLG', 'SATF', 'Satisfied'),

    -- ── ETTT  Estate Type ───────────────────────────────────
    ('ETTT', 'FSIM', 'Fee Simple'),
    ('ETTT', 'LIFE', 'Life Estate'),
    ('ETTT', 'LEAS', 'Leasehold'),
    ('ETTT', 'UNIT', 'Unit Title'),
    ('ETTT', 'RCLS', 'Remainder / Contingent Life Estate'),
    ('ETTT', 'STRM', 'Stratum Estate'),
    ('ETTT', 'TIMR', 'Timeshare'),
    ('ETTT', 'PRTL', 'Partial'),

    -- ── TSDS  General Record Status ─────────────────────────
    ('TSDS', 'LIVE', 'Live'),
    ('TSDS', 'HIST', 'Historical'),
    ('TSDS', 'CANC', 'Cancelled'),
    ('TSDS', 'REGD', 'Registered')

) v(CODE_GROUP, CODE, DESCRIPTION)
ON  t.CODE_GROUP = v.CODE_GROUP
AND t.CODE       = v.CODE
WHEN NOT MATCHED THEN
    INSERT (CODE_GROUP, CODE, DESCRIPTION)
    VALUES (v.CODE_GROUP, v.CODE, v.DESCRIPTION)
WHEN MATCHED AND t.DESCRIPTION IS NULL THEN
    UPDATE SET t.DESCRIPTION = v.DESCRIPTION;

-- ============================================================
-- Smoke check
-- ============================================================
SELECT CODE_GROUP, COUNT(*) AS code_count
FROM SILVER.SYSTEM_CODE
GROUP BY CODE_GROUP
ORDER BY CODE_GROUP;
