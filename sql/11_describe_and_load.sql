USE DATABASE L;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- Stored procedure: read DBF header from inside a staged ZIP
-- ============================================================
CREATE OR REPLACE PROCEDURE OPS.DESCRIBE_DBF(STAGE_ZIP STRING, DBF_PATH STRING)
RETURNS TABLE (ORDINAL NUMBER, FIELD_NAME STRING, FIELD_TYPE STRING, FIELD_LENGTH NUMBER)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process'
AS $$
import zipfile, io, struct

def process(session, stage_zip, dbf_path):
    from snowflake.snowpark.files import SnowflakeFile
    with SnowflakeFile.open(stage_zip, 'rb', require_scoped_url=False) as f:
        zip_bytes = f.read()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(dbf_path) as entry:
            header       = entry.read(32)
            header_size  = struct.unpack_from('<H', header, 8)[0]
            rest         = entry.read(header_size - 32)
    fields, pos, i = [], 0, 0
    NULL = b'\x00'
    while pos < len(rest) - 1 and rest[pos:pos+1] != b'\r':
        name   = rest[pos:pos+11].rstrip(NULL).decode('utf-8', errors='replace')
        ftype  = chr(rest[pos+11])
        length = rest[pos+16]
        fields.append((i, name, ftype, length))
        pos += 32
        i   += 1
    return session.create_dataframe(fields, schema=['ORDINAL','FIELD_NAME','FIELD_TYPE','FIELD_LENGTH'])
$$;

-- ============================================================
-- Inspect column names for every relevant entity
-- ============================================================
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-transaction-type/landonline-transaction-type.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-title/landonline-title.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-title-estate/landonline-title-estate.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-title-instrument/landonline-title-instrument.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-title-instrument-title/landonline-title-instrument-title.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-title-encumbrance/landonline-title-encumbrance.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-title-hierarchy/landonline-title-hierarchy.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-encumbrance/landonline-encumbrance.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip', 'landonline-encumbrance-share/landonline-encumbrance-share.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',  'landonline-appellation/landonline-appellation.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',  'landonline-legal-description/landonline-legal-description.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',  'landonline-legal-description-parcel/landonline-legal-description-parcel.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',  'landonline-title-parcel-association/landonline-title-parcel-association.dbf');
CALL OPS.DESCRIBE_DBF('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',  'landonline-statute/landonline-statute.dbf');
