-- ============================================================
-- 16_populate_is_current.sql
-- Populate IS_CURRENT computed boolean from STATUS codes
-- Run after all Silver tables are loaded.
-- ============================================================

USE DATABASE L;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- TITLE: current = 'LIVE'
UPDATE SILVER.TITLE
SET IS_CURRENT = (STATUS = 'LIVE');

SELECT 'TITLE IS_CURRENT' AS check_name,
       SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END)       AS is_current_true,
       SUM(CASE WHEN NOT IS_CURRENT THEN 1 ELSE 0 END)   AS is_current_false,
       COUNT(*) AS total
FROM SILVER.TITLE;

-- TITLE_ESTATE: current = 'REGD'
UPDATE SILVER.TITLE_ESTATE
SET IS_CURRENT = (STATUS = 'REGD');

SELECT 'TITLE_ESTATE IS_CURRENT' AS check_name,
       SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END)       AS is_current_true,
       SUM(CASE WHEN NOT IS_CURRENT THEN 1 ELSE 0 END)   AS is_current_false,
       COUNT(*) AS total
FROM SILVER.TITLE_ESTATE;

-- TITLE_INSTRUMENT: current = 'REGD'
UPDATE SILVER.TITLE_INSTRUMENT
SET IS_CURRENT = (STATUS = 'REGD');

SELECT 'TITLE_INSTRUMENT IS_CURRENT' AS check_name,
       SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END)       AS is_current_true,
       SUM(CASE WHEN NOT IS_CURRENT THEN 1 ELSE 0 END)   AS is_current_false,
       COUNT(*) AS total
FROM SILVER.TITLE_INSTRUMENT;

-- TITLE_ENCUMBRANCE: current = 'REGD'
UPDATE SILVER.TITLE_ENCUMBRANCE
SET IS_CURRENT = (STATUS = 'REGD');

SELECT 'TITLE_ENCUMBRANCE IS_CURRENT' AS check_name,
       SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END)       AS is_current_true,
       SUM(CASE WHEN NOT IS_CURRENT THEN 1 ELSE 0 END)   AS is_current_false,
       COUNT(*) AS total
FROM SILVER.TITLE_ENCUMBRANCE;

-- ENCUMBRANCE: current = 'REGD'
UPDATE SILVER.ENCUMBRANCE
SET IS_CURRENT = (STATUS = 'REGD');

SELECT 'ENCUMBRANCE IS_CURRENT' AS check_name,
       SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END)       AS is_current_true,
       SUM(CASE WHEN NOT IS_CURRENT THEN 1 ELSE 0 END)   AS is_current_false,
       COUNT(*) AS total
FROM SILVER.ENCUMBRANCE;

-- ESTATE_SHARE: current = 'REGD' (will apply once table is loaded)
UPDATE SILVER.ESTATE_SHARE
SET IS_CURRENT = (STATUS = 'REGD');

-- ============================================================
-- Verify: RPT_ACTIVE_ENCUMBRANCES should now have rows
-- ============================================================
SELECT 'RPT_ACTIVE_ENCUMBRANCES' AS check_name, COUNT(*) AS row_count
FROM GOLD.RPT_ACTIVE_ENCUMBRANCES;

SELECT 'FACT_TITLE_ESTATE_current' AS check_name, COUNT(*) AS row_count
FROM GOLD.FACT_TITLE_ESTATE
WHERE IS_CURRENT = TRUE;
