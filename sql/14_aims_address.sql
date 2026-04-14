-- AIMS address data schema + GOLD.DIM_ADDRESS view
-- Source: lds-world-14layers-SHP (deprecated AIMS address system)
-- Link chain: AIMS_ADDRESS.parcel_id → TITLE_PARCEL_ASSOCIATION.par_id → TITLE.title_no

USE DATABASE L;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- SILVER tables
-- ============================================================
CREATE TABLE IF NOT EXISTS SILVER.AIMS_ADDRESS (
  ADDRESS_ID      NUMBER,
  PARCEL_ID       NUMBER,        -- FK → SILVER.PARCEL.ID / TITLE_PARCEL_ASSOCIATION.PAR_ID
  ADDRESSABLE_ID  NUMBER,
  STATUS          STRING,        -- lifecycle: Current / Retired
  PRODUCER        STRING,
  MAINTAINER      STRING,
  ADDRESS_CLASS   STRING,        -- Road / Water
  LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_AIMS_ADDRESS PRIMARY KEY (ADDRESS_ID)
);

CREATE TABLE IF NOT EXISTS SILVER.AIMS_ADDRESS_COMPONENT (
  COMPONENT_ID    NUMBER,
  ADDRESS_ID      NUMBER,        -- FK → AIMS_ADDRESS.ADDRESS_ID
  COMPONENT_TYPE  STRING,        -- e.g. 'Road Name', 'Suburb/Locality Name'
  COMPONENT_VALUE STRING,
  COMPONENT_SEQ   STRING,
  COMPONENT_GROUP STRING,
  BEGIN_LIFECYCLE STRING,        -- stored as YYYYMMDD string from DBF
  END_LIFECYCLE   STRING,        -- '00000000' = still current
  LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_AIMS_ADDRESS_COMPONENT PRIMARY KEY (COMPONENT_ID)
);

-- ============================================================
-- GOLD view: one readable address row per title
-- ============================================================
CREATE OR REPLACE VIEW GOLD.DIM_ADDRESS AS
WITH current_components AS (
  SELECT address_id, component_type, component_value
  FROM SILVER.AIMS_ADDRESS_COMPONENT
  WHERE end_lifecycle IS NULL
     OR end_lifecycle IN ('', '00000000')
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
    MAX(CASE WHEN component_type = 'Road Suffix Name'      THEN component_value END) AS road_suffix,
    MAX(CASE WHEN component_type = 'Suburb/Locality Name'  THEN component_value END) AS suburb,
    MAX(CASE WHEN component_type = 'Town/City Name'        THEN component_value END) AS city,
    MAX(CASE WHEN component_type = 'Postcode'              THEN component_value END) AS postcode
  FROM current_components
  GROUP BY address_id
)
SELECT
  a.address_id,
  a.parcel_id,
  tpa.ttl_title_no                                                AS title_no,
  -- Unit prefix e.g. "Flat 3"
  NULLIF(TRIM(CONCAT_WS(' ', p.unit_type, p.unit_value)), '')    AS unit,
  -- Number e.g. "14" or "14-20"
  NULLIF(CONCAT_WS('-', p.street_number, p.street_number_high), '-') AS street_number,
  p.street_number_suffix,
  p.road_name,
  p.road_type,
  p.road_suffix,
  -- Full single-line address
  TRIM(CONCAT_WS(' ',
    NULLIF(TRIM(CONCAT_WS(' ', p.unit_type, p.unit_value)), ''),
    NULLIF(CONCAT_WS('-', p.street_number, p.street_number_high), '-'),
    p.street_number_suffix,
    p.road_name,
    p.road_type,
    p.road_suffix
  ))                                                              AS street_address,
  p.suburb,
  p.city,
  p.postcode,
  -- Full formatted address
  TRIM(CONCAT_WS(', ',
    TRIM(CONCAT_WS(' ',
      NULLIF(TRIM(CONCAT_WS(' ', p.unit_type, p.unit_value)), ''),
      NULLIF(CONCAT_WS('-', p.street_number, p.street_number_high), '-'),
      p.street_number_suffix,
      p.road_name,
      p.road_type,
      p.road_suffix
    )),
    p.suburb,
    p.city,
    p.postcode
  ))                                                              AS full_address,
  a.status                                                        AS lifecycle_status
FROM SILVER.AIMS_ADDRESS a
JOIN pivoted p           ON a.address_id   = p.address_id
LEFT JOIN SILVER.TITLE_PARCEL_ASSOCIATION tpa ON a.parcel_id = tpa.par_id
WHERE a.status = 'Current';
