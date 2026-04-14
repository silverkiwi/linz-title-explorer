-- ============================================================
-- 17_resume_load.sql
-- Resume loading from where 13_fixed_loader.sql stopped.
-- Does NOT truncate — appends to existing partial data.
-- Tables already complete: TITLE, TITLE_ESTATE, TITLE_INSTRUMENT(pt1),
--   TITLE_INSTRUMENT_TITLE, TITLE_ENCUMBRANCE, TITLE_HIERARCHY,
--   ENCUMBRANCE, ENCUMBRANCE_SHARE, APPELLATION(pt1), TRANSACTION_TYPE
-- ============================================================

USE DATABASE L;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- TITLE_INSTRUMENT split 2 (split 1 at 18.5M already loaded)
CALL OPS.LOAD_DBF_TO_TABLE(
    '@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip',
    'landonline-title-instrument/landonline-title-instrument.2.dbf',
    'SILVER', 'TITLE_INSTRUMENT',
    '{"id":"ID","inst_no":"INST_NO","trt_grp":"TRT_GRP","trt_type":"TRT_TYPE","ldt_loc_id":"LDT_LOC_ID","status":"STATUS","lodged_dat":"LODGED_DATETIME","dlg_id":"DLG_ID","priority_n":"PRIORITY_NO","tin_id_par":"TIN_ID_PARENT","audit_id":"AUDIT_ID"}',
    50000
);
SELECT 'TITLE_INSTRUMENT after split 2' AS step, COUNT(*) AS cnt FROM SILVER.TITLE_INSTRUMENT;

-- APPELLATION split 2
CALL OPS.LOAD_DBF_TO_TABLE(
    '@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',
    'landonline-appellation/landonline-appellation.2.dbf',
    'SILVER', 'APPELLATION',
    '{"par_id":"PAR_ID","type":"TYPE","title":"TITLE_FLAG","survey":"SURVEY_FLAG","status":"STATUS","part_indic":"PART_INDICATOR","maori_name":"MAORI_NAME","sub_type":"SUB_TYPE","appellatio":"APPELLATION_VALUE","parcel_typ":"PARCEL_TYPE","parcel_val":"PARCEL_VALUE","second_par":"SECOND_PARCEL_TYPE","second_prc":"SECOND_PRCL_VALUE","block_numb":"BLOCK_NUMBER","sub_type_p":"SUB_TYPE_POSITION","other_appe":"OTHER_APPELLATION","act_id_crt":"ACT_ID_CRT","act_tin_id":"ACT_TIN_ID_CRT","act_id_ext":"ACT_ID_EXT","act_tin__1":"ACT_TIN_ID_EXT","id":"ID","audit_id":"AUDIT_ID","height_lim":"HEIGHT_LIMITED"}',
    50000
);

-- APPELLATION split 3
CALL OPS.LOAD_DBF_TO_TABLE(
    '@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',
    'landonline-appellation/landonline-appellation.3.dbf',
    'SILVER', 'APPELLATION',
    '{"par_id":"PAR_ID","type":"TYPE","title":"TITLE_FLAG","survey":"SURVEY_FLAG","status":"STATUS","part_indic":"PART_INDICATOR","maori_name":"MAORI_NAME","sub_type":"SUB_TYPE","appellatio":"APPELLATION_VALUE","parcel_typ":"PARCEL_TYPE","parcel_val":"PARCEL_VALUE","second_par":"SECOND_PARCEL_TYPE","second_prc":"SECOND_PRCL_VALUE","block_numb":"BLOCK_NUMBER","sub_type_p":"SUB_TYPE_POSITION","other_appe":"OTHER_APPELLATION","act_id_crt":"ACT_ID_CRT","act_tin_id":"ACT_TIN_ID_CRT","act_id_ext":"ACT_ID_EXT","act_tin__1":"ACT_TIN_ID_EXT","id":"ID","audit_id":"AUDIT_ID","height_lim":"HEIGHT_LIMITED"}',
    50000
);
SELECT 'APPELLATION after all splits' AS step, COUNT(*) AS cnt FROM SILVER.APPELLATION;

-- LEGAL_DESCRIPTION split 1
CALL OPS.LOAD_DBF_TO_TABLE(
    '@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',
    'landonline-legal-description/landonline-legal-description.dbf',
    'SILVER', 'LEGAL_DESCRIPTION',
    '{"id":"ID","type":"TYPE","status":"STATUS","total_area":"TOTAL_AREA","ttl_title_":"TTL_TITLE_NO","legal_desc":"LEGAL_DESC_TEXT","audit_id":"AUDIT_ID"}',
    50000
);

-- LEGAL_DESCRIPTION split 2
CALL OPS.LOAD_DBF_TO_TABLE(
    '@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',
    'landonline-legal-description/landonline-legal-description.2.dbf',
    'SILVER', 'LEGAL_DESCRIPTION',
    '{"id":"ID","type":"TYPE","status":"STATUS","total_area":"TOTAL_AREA","ttl_title_":"TTL_TITLE_NO","legal_desc":"LEGAL_DESC_TEXT","audit_id":"AUDIT_ID"}',
    50000
);
SELECT 'LEGAL_DESCRIPTION after all splits' AS step, COUNT(*) AS cnt FROM SILVER.LEGAL_DESCRIPTION;

-- LEGAL_DESCRIPTION_PARCEL
CALL OPS.LOAD_DBF_TO_TABLE(
    '@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',
    'landonline-legal-description-parcel/landonline-legal-description-parcel.dbf',
    'SILVER', 'LEGAL_DESCRIPTION_PARCEL',
    '{"lgd_id":"LGD_ID","par_id":"PAR_ID","sequence":"SEQUENCE","part_affec":"PART_AFFECTED","share":"SHARE","audit_id":"AUDIT_ID","sur_wrk_id":"SUR_WRK_ID"}',
    50000
);
SELECT 'LEGAL_DESCRIPTION_PARCEL' AS step, COUNT(*) AS cnt FROM SILVER.LEGAL_DESCRIPTION_PARCEL;

-- TITLE_PARCEL_ASSOCIATION (the key one — unlocks address/appellation joins)
CALL OPS.LOAD_DBF_TO_TABLE(
    '@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',
    'landonline-title-parcel-association/landonline-title-parcel-association.dbf',
    'SILVER', 'TITLE_PARCEL_ASSOCIATION',
    '{"id":"ID","ttl_title_":"TTL_TITLE_NO","par_id":"PAR_ID","source":"SOURCE"}',
    50000
);
SELECT 'TITLE_PARCEL_ASSOCIATION' AS step, COUNT(*) AS cnt FROM SILVER.TITLE_PARCEL_ASSOCIATION;

-- Fix IS_CURRENT on TITLE_INSTRUMENT now that both splits are loaded
UPDATE SILVER.TITLE_INSTRUMENT SET IS_CURRENT = (STATUS = 'REGD');

-- Final counts across all silver tables
SELECT tbl, cnt FROM (
  SELECT 'TITLE'                    AS tbl, COUNT(*) AS cnt FROM SILVER.TITLE
  UNION ALL SELECT 'TITLE_ESTATE',                              COUNT(*) FROM SILVER.TITLE_ESTATE
  UNION ALL SELECT 'TITLE_INSTRUMENT',                          COUNT(*) FROM SILVER.TITLE_INSTRUMENT
  UNION ALL SELECT 'TITLE_INSTRUMENT_TITLE',                    COUNT(*) FROM SILVER.TITLE_INSTRUMENT_TITLE
  UNION ALL SELECT 'TITLE_ENCUMBRANCE',                         COUNT(*) FROM SILVER.TITLE_ENCUMBRANCE
  UNION ALL SELECT 'TITLE_HIERARCHY',                           COUNT(*) FROM SILVER.TITLE_HIERARCHY
  UNION ALL SELECT 'TITLE_PARCEL_ASSOCIATION',                  COUNT(*) FROM SILVER.TITLE_PARCEL_ASSOCIATION
  UNION ALL SELECT 'ENCUMBRANCE',                               COUNT(*) FROM SILVER.ENCUMBRANCE
  UNION ALL SELECT 'ENCUMBRANCE_SHARE',                         COUNT(*) FROM SILVER.ENCUMBRANCE_SHARE
  UNION ALL SELECT 'APPELLATION',                               COUNT(*) FROM SILVER.APPELLATION
  UNION ALL SELECT 'LEGAL_DESCRIPTION',                         COUNT(*) FROM SILVER.LEGAL_DESCRIPTION
  UNION ALL SELECT 'LEGAL_DESCRIPTION_PARCEL',                  COUNT(*) FROM SILVER.LEGAL_DESCRIPTION_PARCEL
  UNION ALL SELECT 'TRANSACTION_TYPE',                          COUNT(*) FROM SILVER.TRANSACTION_TYPE
  UNION ALL SELECT 'AIMS_ADDRESS',                              COUNT(*) FROM SILVER.AIMS_ADDRESS
  UNION ALL SELECT 'AIMS_ADDRESS_COMPONENT',                    COUNT(*) FROM SILVER.AIMS_ADDRESS_COMPONENT
) ORDER BY tbl;
