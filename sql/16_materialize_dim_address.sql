-- ============================================================
-- 16_materialize_dim_address.sql
-- Materialise GOLD.DIM_ADDRESS as a physical table so that
-- address searches are near-instant instead of re-running the
-- full pivot + join on every query.
--
-- Three complementary optimisations:
--   1. Physical table  — eliminates the per-query pivot/join cost
--   2. Pre-normalised columns (lowercase + macrons stripped)
--      — avoids per-row LOWER(TRANSLATE(...)) expressions so
--        Snowflake's Search Optimization can take effect
--   3. Search Optimization (SUBSTRING) on full_address_norm
--      and road_name_norm — turns leading-wildcard LIKE into an
--        O(log n) index lookup instead of a full-table scan
--
-- Run once after initial load; thereafter the TASK
-- OPS.REFRESH_DIM_ADDRESS_SEARCH keeps the table current.
-- ============================================================

USE DATABASE L;
USE SCHEMA GOLD;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1) Materialised search table
-- ============================================================
CREATE OR REPLACE TABLE GOLD.DIM_ADDRESS_SEARCH AS
SELECT
    a.address_id,
    a.parcel_id,
    a.title_no,
    a.unit_type,
    a.unit_value,
    a.street_number,
    a.street_number_high,
    a.street_number_suffix,
    a.road_name,
    a.road_type,
    a.suburb,
    a.city,
    a.postcode,
    a.street_address,
    a.full_address,
    a.status,
    a.address_class,
    -- Pre-normalised: lowercase + Māori macrons → ASCII.
    -- These are the columns actually searched at query time;
    -- storing them avoids calling LOWER(TRANSLATE(...)) on
    -- every row for every search, and lets Search Optimization work.
    LOWER(TRANSLATE(a.full_address,   'āēīōūĀĒĪŌŪ', 'aeiouAEIOU')) AS full_address_norm,
    LOWER(TRANSLATE(a.road_name,      'āēīōūĀĒĪŌŪ', 'aeiouAEIOU')) AS road_name_norm,
    LOWER(COALESCE(a.street_number, ''))                             AS street_number_norm,
    LOWER(COALESCE(a.unit_value, ''))                               AS unit_value_norm
FROM GOLD.DIM_ADDRESS a
WHERE a.title_no IS NOT NULL;

-- ============================================================
-- 2) Clustering key — prunes micro-partitions on prefix queries
--    and keeps rows with the same address prefix co-located.
-- ============================================================
ALTER TABLE GOLD.DIM_ADDRESS_SEARCH
    CLUSTER BY (full_address_norm);

-- ============================================================
-- 3) Search Optimization — builds a server-side search access
--    path that makes LIKE '%substring%' queries O(log n).
--    Both full_address_norm and road_name_norm are searched.
-- ============================================================
ALTER TABLE GOLD.DIM_ADDRESS_SEARCH
    ADD SEARCH OPTIMIZATION ON
        SUBSTRING(full_address_norm),
        SUBSTRING(road_name_norm);

-- ============================================================
-- 4) Scheduled refresh task
--    Runs nightly at 03:00 NZST (15:00 UTC) to pick up any
--    new LINZ data loaded during the day.
--    Resume the task after creation (tasks start SUSPENDED).
-- ============================================================
CREATE OR REPLACE TASK OPS.REFRESH_DIM_ADDRESS_SEARCH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 15 * * * UTC'
    COMMENT   = 'Nightly rebuild of GOLD.DIM_ADDRESS_SEARCH (03:00 NZST)'
AS
    CREATE OR REPLACE TABLE GOLD.DIM_ADDRESS_SEARCH AS
    SELECT
        a.address_id,
        a.parcel_id,
        a.title_no,
        a.unit_type,
        a.unit_value,
        a.street_number,
        a.street_number_high,
        a.street_number_suffix,
        a.road_name,
        a.road_type,
        a.suburb,
        a.city,
        a.postcode,
        a.street_address,
        a.full_address,
        a.status,
        a.address_class,
        LOWER(TRANSLATE(a.full_address,   'āēīōūĀĒĪŌŪ', 'aeiouAEIOU')) AS full_address_norm,
        LOWER(TRANSLATE(a.road_name,      'āēīōūĀĒĪŌŪ', 'aeiouAEIOU')) AS road_name_norm,
        LOWER(COALESCE(a.street_number, ''))                             AS street_number_norm,
        LOWER(COALESCE(a.unit_value, ''))                               AS unit_value_norm
    FROM GOLD.DIM_ADDRESS a
    WHERE a.title_no IS NOT NULL;

ALTER TASK OPS.REFRESH_DIM_ADDRESS_SEARCH RESUME;

-- ============================================================
-- 5) Smoke check
-- ============================================================
SELECT
    'DIM_ADDRESS_SEARCH row count'  AS check_name,
    COUNT(*)                        AS row_count
FROM GOLD.DIM_ADDRESS_SEARCH

UNION ALL

SELECT
    'search optimisation status',
    COUNT(*)
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    TABLE_NAME       => 'DIM_ADDRESS_SEARCH'
));
