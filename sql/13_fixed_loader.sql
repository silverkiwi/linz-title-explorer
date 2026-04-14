-- Fixed streaming DBF loader:
--   1. Proper type conversion (N/F → int/float, D → date string, * → NULL)
--   2. Larger batch size (50k rows) to reduce write_pandas overhead

USE DATABASE L;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE PROCEDURE OPS.LOAD_DBF_TO_TABLE(
    STAGE_ZIP     STRING,
    DBF_PATH      STRING,
    TARGET_SCHEMA STRING,
    TARGET_TABLE  STRING,
    COL_MAP_JSON  STRING,
    BATCH_SIZE    NUMBER
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'process'
AS $$
import zipfile, io, struct, json
import pandas as pd

NULL_CHARS = {' ', '*', '\x00'}

def parse_val(raw_bytes, ftype):
    """Decode a DBF field with proper type coercion."""
    v = raw_bytes.decode('utf-8', errors='replace').strip()
    if not v or all(c in NULL_CHARS for c in v):
        return None
    if ftype in ('N', 'F'):
        try:
            return float(v) if '.' in v else int(v)
        except ValueError:
            return None
    if ftype == 'D':
        # DBF date stored as YYYYMMDD
        if len(v) == 8 and v.isdigit():
            return f"{v[:4]}-{v[4:6]}-{v[6:8]}"
        return None
    if ftype == 'L':
        return v.upper() in ('T', 'Y', '1')
    return v  # C / M / other → string

def process(session, stage_zip, dbf_path, target_schema, target_table, col_map_json, batch_size):
    from snowflake.snowpark.files import SnowflakeFile

    rename     = json.loads(col_map_json)
    batch_size = int(batch_size) if batch_size else 50000

    with SnowflakeFile.open(stage_zip, 'rb', require_scoped_url=False) as f:
        zip_bytes = f.read()

    total_written = 0
    errors        = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(dbf_path) as entry:

            # --- Parse DBF header ---
            hdr         = entry.read(32)
            num_records = struct.unpack_from('<I', hdr, 4)[0]
            header_size = struct.unpack_from('<H', hdr, 8)[0]
            record_size = struct.unpack_from('<H', hdr, 10)[0]
            rest        = entry.read(header_size - 32)

            fields, pos = [], 0
            while pos < len(rest) - 1 and rest[pos:pos+1] != b'\r':
                raw_name = rest[pos:pos+11].rstrip(b'\x00').decode('utf-8', errors='replace')
                ftype    = chr(rest[pos + 11])
                length   = rest[pos + 16]
                fields.append((raw_name, ftype, length))
                pos += 32

            silver_cols = [rename.get(f[0], f[0].upper()) for f in fields]

            def flush(batch):
                nonlocal total_written
                try:
                    pdf = pd.DataFrame(batch, columns=silver_cols)
                    session.write_pandas(
                        pdf, target_table,
                        schema=target_schema, database='L',
                        overwrite=False, auto_create_table=False
                    )
                    total_written += len(batch)
                except Exception as e:
                    errors.append(str(e)[:300])

            # --- Stream records ---
            batch = []
            for _ in range(num_records):
                rec = entry.read(record_size)
                if not rec or len(rec) < record_size or rec[0:1] == b'\x1a':
                    break
                if rec[0:1] != b'*':            # skip deleted records
                    vals, foff = [], 1
                    for _, ftype, length in fields:
                        vals.append(parse_val(rec[foff:foff+length], ftype))
                        foff += length
                    batch.append(vals)
                    if len(batch) >= batch_size:
                        flush(batch)
                        batch = []

            if batch:
                flush(batch)

    msg = f"Loaded {total_written:,} rows into {target_schema}.{target_table}"
    if errors:
        msg += f" | {len(errors)} batch error(s): {errors[0]}"
    return msg
$$;

-- ============================================================
-- Reload all entities (TRUNCATE first to avoid duplicates)
-- ============================================================
TRUNCATE TABLE SILVER.TRANSACTION_TYPE;
TRUNCATE TABLE SILVER.TITLE;
TRUNCATE TABLE SILVER.TITLE_ESTATE;
TRUNCATE TABLE SILVER.TITLE_INSTRUMENT;
TRUNCATE TABLE SILVER.TITLE_INSTRUMENT_TITLE;
TRUNCATE TABLE SILVER.TITLE_ENCUMBRANCE;
TRUNCATE TABLE SILVER.TITLE_HIERARCHY;
TRUNCATE TABLE SILVER.ENCUMBRANCE;
TRUNCATE TABLE SILVER.ENCUMBRANCE_SHARE;
TRUNCATE TABLE SILVER.APPELLATION;
TRUNCATE TABLE SILVER.LEGAL_DESCRIPTION;
TRUNCATE TABLE SILVER.LEGAL_DESCRIPTION_PARCEL;
TRUNCATE TABLE SILVER.TITLE_PARCEL_ASSOCIATION;

-- 10-layers DBF zip
CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-transaction-type/landonline-transaction-type.dbf','SILVER','TRANSACTION_TYPE','{"grp":"GRP","type":"TYPE","descriptio":"DESCRIPTION","audit_id":"AUDIT_ID"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title/landonline-title.dbf','SILVER','TITLE','{"title_no":"TITLE_NO","ldt_loc_id":"LDT_LOC_ID","register_t":"REGISTER_TYPE","ste_id":"STE_ID","issue_date":"ISSUE_DATE","guarantee_":"GUARANTEE_STATUS","status":"STATUS","type":"TYPE","provisiona":"PROVISIONAL","sur_wrk_id":"SUR_WRK_ID","ttl_title_":"TTL_TITLE_NO_SRS","ttl_titl_1":"TTL_TITLE_NO_HEAD_SRS","maori_land":"MAORI_LAND","audit_id":"AUDIT_ID"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title-estate/landonline-title-estate.dbf','SILVER','TITLE_ESTATE','{"id":"ID","ttl_title_":"TTL_TITLE_NO","type":"TYPE","status":"STATUS","lgd_id":"LGD_ID","share":"SHARE","timeshare_":"TIMESHARE_WEEK_NO","purpose":"PURPOSE","act_tin_id":"ACT_TIN_ID_CRT","act_id_crt":"ACT_ID_CRT","original_f":"ORIGINAL_FLAG","term":"TERM","tin_id_ori":"TIN_ID_ORIG"}',50000);
CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title-estate/landonline-title-estate.2.dbf','SILVER','TITLE_ESTATE','{"id":"ID","ttl_title_":"TTL_TITLE_NO","type":"TYPE","status":"STATUS","lgd_id":"LGD_ID","share":"SHARE","timeshare_":"TIMESHARE_WEEK_NO","purpose":"PURPOSE","act_tin_id":"ACT_TIN_ID_CRT","act_id_crt":"ACT_ID_CRT","original_f":"ORIGINAL_FLAG","term":"TERM","tin_id_ori":"TIN_ID_ORIG"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title-instrument/landonline-title-instrument.dbf','SILVER','TITLE_INSTRUMENT','{"id":"ID","inst_no":"INST_NO","trt_grp":"TRT_GRP","trt_type":"TRT_TYPE","ldt_loc_id":"LDT_LOC_ID","status":"STATUS","lodged_dat":"LODGED_DATETIME","dlg_id":"DLG_ID","priority_n":"PRIORITY_NO","tin_id_par":"TIN_ID_PARENT","audit_id":"AUDIT_ID"}',50000);
CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title-instrument/landonline-title-instrument.2.dbf','SILVER','TITLE_INSTRUMENT','{"id":"ID","inst_no":"INST_NO","trt_grp":"TRT_GRP","trt_type":"TRT_TYPE","ldt_loc_id":"LDT_LOC_ID","status":"STATUS","lodged_dat":"LODGED_DATETIME","dlg_id":"DLG_ID","priority_n":"PRIORITY_NO","tin_id_par":"TIN_ID_PARENT","audit_id":"AUDIT_ID"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title-instrument-title/landonline-title-instrument-title.dbf','SILVER','TITLE_INSTRUMENT_TITLE','{"tin_id":"TIN_ID","ttl_title_":"TTL_TITLE_NO","audit_id":"AUDIT_ID"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title-encumbrance/landonline-title-encumbrance.dbf','SILVER','TITLE_ENCUMBRANCE','{"id":"ID","ttl_title_":"TTL_TITLE_NO","enc_id":"ENC_ID","status":"STATUS","act_tin_id":"ACT_TIN_ID_CRT","act_id_crt":"ACT_ID_CRT"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-title-hierarchy/landonline-title-hierarchy.dbf','SILVER','TITLE_HIERARCHY','{"id":"ID","status":"STATUS","ttl_title_":"TTL_TITLE_NO_PRIOR","ttl_titl_1":"TTL_TITLE_NO_FLW","tdr_id":"TDR_ID","act_tin_id":"ACT_TIN_ID_CRT","act_id_crt":"ACT_ID_CRT"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-encumbrance/landonline-encumbrance.dbf','SILVER','ENCUMBRANCE','{"id":"ID","status":"STATUS","act_tin_id":"ACT_TIN_ID_CRT","act_tin__1":"ACT_TIN_ID_ORIG","act_id_crt":"ACT_ID_CRT","act_id_ori":"ACT_ID_ORIG","term":"TERM"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip','landonline-encumbrance-share/landonline-encumbrance-share.dbf','SILVER','ENCUMBRANCE_SHARE','{"id":"ID","enc_id":"ENC_ID","status":"STATUS","act_tin_id":"ACT_TIN_ID_CRT","act_id_crt":"ACT_ID_CRT","act_id_ext":"ACT_ID_EXT","act_tin__1":"ACT_TIN_ID_EXT","system_crt":"SYSTEM_CRT","system_ext":"SYSTEM_EXT"}',50000);

-- 7-layers SHP zip
CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip','landonline-appellation/landonline-appellation.dbf','SILVER','APPELLATION','{"par_id":"PAR_ID","type":"TYPE","title":"TITLE_FLAG","survey":"SURVEY_FLAG","status":"STATUS","part_indic":"PART_INDICATOR","maori_name":"MAORI_NAME","sub_type":"SUB_TYPE","appellatio":"APPELLATION_VALUE","parcel_typ":"PARCEL_TYPE","parcel_val":"PARCEL_VALUE","second_par":"SECOND_PARCEL_TYPE","second_prc":"SECOND_PRCL_VALUE","block_numb":"BLOCK_NUMBER","sub_type_p":"SUB_TYPE_POSITION","other_appe":"OTHER_APPELLATION","act_id_crt":"ACT_ID_CRT","act_tin_id":"ACT_TIN_ID_CRT","act_id_ext":"ACT_ID_EXT","act_tin__1":"ACT_TIN_ID_EXT","id":"ID","audit_id":"AUDIT_ID","height_lim":"HEIGHT_LIMITED"}',50000);
CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip','landonline-appellation/landonline-appellation.2.dbf','SILVER','APPELLATION','{"par_id":"PAR_ID","type":"TYPE","title":"TITLE_FLAG","survey":"SURVEY_FLAG","status":"STATUS","part_indic":"PART_INDICATOR","maori_name":"MAORI_NAME","sub_type":"SUB_TYPE","appellatio":"APPELLATION_VALUE","parcel_typ":"PARCEL_TYPE","parcel_val":"PARCEL_VALUE","second_par":"SECOND_PARCEL_TYPE","second_prc":"SECOND_PRCL_VALUE","block_numb":"BLOCK_NUMBER","sub_type_p":"SUB_TYPE_POSITION","other_appe":"OTHER_APPELLATION","act_id_crt":"ACT_ID_CRT","act_tin_id":"ACT_TIN_ID_CRT","act_id_ext":"ACT_ID_EXT","act_tin__1":"ACT_TIN_ID_EXT","id":"ID","audit_id":"AUDIT_ID","height_lim":"HEIGHT_LIMITED"}',50000);
CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip','landonline-appellation/landonline-appellation.3.dbf','SILVER','APPELLATION','{"par_id":"PAR_ID","type":"TYPE","title":"TITLE_FLAG","survey":"SURVEY_FLAG","status":"STATUS","part_indic":"PART_INDICATOR","maori_name":"MAORI_NAME","sub_type":"SUB_TYPE","appellatio":"APPELLATION_VALUE","parcel_typ":"PARCEL_TYPE","parcel_val":"PARCEL_VALUE","second_par":"SECOND_PARCEL_TYPE","second_prc":"SECOND_PRCL_VALUE","block_numb":"BLOCK_NUMBER","sub_type_p":"SUB_TYPE_POSITION","other_appe":"OTHER_APPELLATION","act_id_crt":"ACT_ID_CRT","act_tin_id":"ACT_TIN_ID_CRT","act_id_ext":"ACT_ID_EXT","act_tin__1":"ACT_TIN_ID_EXT","id":"ID","audit_id":"AUDIT_ID","height_lim":"HEIGHT_LIMITED"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip','landonline-legal-description/landonline-legal-description.dbf','SILVER','LEGAL_DESCRIPTION','{"id":"ID","type":"TYPE","status":"STATUS","total_area":"TOTAL_AREA","ttl_title_":"TTL_TITLE_NO","legal_desc":"LEGAL_DESC_TEXT","audit_id":"AUDIT_ID"}',50000);
CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip','landonline-legal-description/landonline-legal-description.2.dbf','SILVER','LEGAL_DESCRIPTION','{"id":"ID","type":"TYPE","status":"STATUS","total_area":"TOTAL_AREA","ttl_title_":"TTL_TITLE_NO","legal_desc":"LEGAL_DESC_TEXT","audit_id":"AUDIT_ID"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip','landonline-legal-description-parcel/landonline-legal-description-parcel.dbf','SILVER','LEGAL_DESCRIPTION_PARCEL','{"lgd_id":"LGD_ID","par_id":"PAR_ID","sequence":"SEQUENCE","part_affec":"PART_AFFECTED","share":"SHARE","audit_id":"AUDIT_ID","sur_wrk_id":"SUR_WRK_ID"}',50000);

CALL OPS.LOAD_DBF_TO_TABLE('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip','landonline-title-parcel-association/landonline-title-parcel-association.dbf','SILVER','TITLE_PARCEL_ASSOCIATION','{"id":"ID","ttl_title_":"TTL_TITLE_NO","par_id":"PAR_ID","source":"SOURCE"}',50000);

-- ============================================================
-- Final row counts
-- ============================================================
SELECT entity, row_count FROM (
    SELECT 'TRANSACTION_TYPE'       AS entity, COUNT(*) AS row_count FROM SILVER.TRANSACTION_TYPE       UNION ALL
    SELECT 'TITLE',                            COUNT(*) FROM SILVER.TITLE                               UNION ALL
    SELECT 'TITLE_ESTATE',                     COUNT(*) FROM SILVER.TITLE_ESTATE                       UNION ALL
    SELECT 'TITLE_INSTRUMENT',                 COUNT(*) FROM SILVER.TITLE_INSTRUMENT                   UNION ALL
    SELECT 'TITLE_INSTRUMENT_TITLE',           COUNT(*) FROM SILVER.TITLE_INSTRUMENT_TITLE             UNION ALL
    SELECT 'TITLE_ENCUMBRANCE',                COUNT(*) FROM SILVER.TITLE_ENCUMBRANCE                  UNION ALL
    SELECT 'TITLE_HIERARCHY',                  COUNT(*) FROM SILVER.TITLE_HIERARCHY                    UNION ALL
    SELECT 'ENCUMBRANCE',                      COUNT(*) FROM SILVER.ENCUMBRANCE                        UNION ALL
    SELECT 'ENCUMBRANCE_SHARE',                COUNT(*) FROM SILVER.ENCUMBRANCE_SHARE                  UNION ALL
    SELECT 'APPELLATION',                      COUNT(*) FROM SILVER.APPELLATION                        UNION ALL
    SELECT 'LEGAL_DESCRIPTION',                COUNT(*) FROM SILVER.LEGAL_DESCRIPTION                  UNION ALL
    SELECT 'LEGAL_DESCRIPTION_PARCEL',         COUNT(*) FROM SILVER.LEGAL_DESCRIPTION_PARCEL           UNION ALL
    SELECT 'TITLE_PARCEL_ASSOCIATION',         COUNT(*) FROM SILVER.TITLE_PARCEL_ASSOCIATION
) ORDER BY row_count DESC;
