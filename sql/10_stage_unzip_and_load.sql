-- Step 1: Inspect ZIP contents (list layers inside each archive)
-- Step 2: Extract DBF → CSV server-side via Snowpark stored procedure
-- Step 3: COPY INTO matching SILVER tables

USE DATABASE L;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1) Stored procedure: list files inside a staged ZIP
--    Usage: CALL OPS.LIST_ZIP_CONTENTS('@linz_stage/lds-world-10layers-DBF.zip');
-- ============================================================
CREATE OR REPLACE PROCEDURE OPS.LIST_ZIP_CONTENTS(STAGE_FILE_PATH STRING)
RETURNS TABLE (FILE_NAME STRING, FILE_SIZE_BYTES NUMBER, IS_DBF BOOLEAN, IS_SHP BOOLEAN)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'ListZipHandler'
AS $$
import zipfile, io

class ListZipHandler:
    def process(self, session, stage_file_path):
        from snowflake.snowpark.files import SnowflakeFile
        with SnowflakeFile.open(stage_file_path, 'rb') as f:
            data = f.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for info in zf.infolist():
                name = info.filename
                lower = name.lower()
                yield (name, info.file_size, lower.endswith('.dbf'), lower.endswith('.shp'))
$$;

-- ============================================================
-- 2) Stored procedure: extract DBF files from a ZIP → CSV files
--    back on the same stage.
--    Usage: CALL OPS.EXTRACT_DBF_TO_CSV('@linz_stage/lds-world-10layers-DBF.zip', '@linz_stage/csv/');
-- ============================================================
CREATE OR REPLACE PROCEDURE OPS.EXTRACT_DBF_TO_CSV(
    STAGE_ZIP_PATH  STRING,
    TARGET_STAGE    STRING
)
RETURNS TABLE (CSV_FILE STRING, ROWS_WRITTEN NUMBER, COLUMNS NUMBER, STATUS STRING)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'ExtractDbfHandler'
AS $$
import zipfile, io, csv, struct

# ---------------------------------------------------------------------------
# Minimal pure-Python DBF reader (no external packages required)
# Supports field types: C (character), N (numeric), D (date), L (logical)
# ---------------------------------------------------------------------------
def parse_dbf(data: bytes):
    if len(data) < 32:
        return [], []

    num_records  = struct.unpack_from('<I', data, 4)[0]
    header_size  = struct.unpack_from('<H', data, 8)[0]
    record_size  = struct.unpack_from('<H', data, 10)[0]

    # Field descriptors start at byte 32, each 32 bytes, terminated by 0x0D
    fields = []
    pos = 32
    while pos < header_size - 1 and data[pos] != 0x0D:
        raw_name = data[pos:pos+11]
        name     = raw_name.rstrip(b'\x00').decode('utf-8', errors='replace')
        ftype    = chr(data[pos + 11])
        length   = data[pos + 16]
        fields.append((name, ftype, length))
        pos += 32

    col_names = [f[0] for f in fields]
    rows = []

    offset = header_size
    for _ in range(num_records):
        if offset >= len(data) or data[offset] == 0x1A:   # EOF marker
            break
        deleted = (data[offset] == 0x2A)                  # * = deleted record
        if not deleted:
            record = []
            foff = offset + 1                             # skip deletion flag byte
            for _, ftype, length in fields:
                raw   = data[foff:foff + length]
                value = raw.decode('utf-8', errors='replace').strip()
                record.append(value)
                foff += length
            rows.append(record)
        offset += record_size

    return col_names, rows


class ExtractDbfHandler:
    def process(self, session, stage_zip_path, target_stage):
        from snowflake.snowpark.files import SnowflakeFile

        # Read the entire ZIP into memory
        with SnowflakeFile.open(stage_zip_path, 'rb') as f:
            zip_bytes = f.read()

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            dbf_names = [n for n in zf.namelist() if n.lower().endswith('.dbf')]

            for dbf_name in dbf_names:
                try:
                    dbf_data = zf.read(dbf_name)
                    col_names, rows = parse_dbf(dbf_data)

                    if not col_names:
                        yield (dbf_name, 0, 0, 'EMPTY_OR_UNREADABLE')
                        continue

                    # Serialise to CSV in memory
                    buf = io.StringIO()
                    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(col_names)
                    writer.writerows(rows)
                    csv_bytes = buf.getvalue().encode('utf-8')

                    # Derive output file name: strip directory prefix, change extension
                    base = dbf_name.split('/')[-1]          # last path component
                    csv_name = base.rsplit('.', 1)[0] + '.csv'
                    dest = target_stage.rstrip('/') + '/' + csv_name

                    session.file.put_stream(
                        io.BytesIO(csv_bytes),
                        dest,
                        auto_compress=False,
                        overwrite=True
                    )
                    yield (csv_name, len(rows), len(col_names), 'OK')

                except Exception as e:
                    yield (dbf_name, 0, 0, f'ERROR: {e}')
$$;

-- ============================================================
-- 3) Inspect what layers are inside each ZIP (run these first)
-- ============================================================
CALL OPS.LIST_ZIP_CONTENTS('@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip');
CALL OPS.LIST_ZIP_CONTENTS('@L.PUBLIC.LINZ_STAGE/lds-world-14layers-SHP.zip');
CALL OPS.LIST_ZIP_CONTENTS('@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip');

-- ============================================================
-- 4) Extract all DBF layers to a csv/ prefix on the same stage
--    (run after reviewing step 3 output)
-- ============================================================
CALL OPS.EXTRACT_DBF_TO_CSV(
    '@L.PUBLIC.LINZ_STAGE/lds-world-10layers-DBF.zip',
    '@L.PUBLIC.LINZ_STAGE/csv/'
);

-- SHP zips also contain paired .dbf attribute tables — extract those too
CALL OPS.EXTRACT_DBF_TO_CSV(
    '@L.PUBLIC.LINZ_STAGE/lds-world-14layers-SHP.zip',
    '@L.PUBLIC.LINZ_STAGE/csv/'
);

CALL OPS.EXTRACT_DBF_TO_CSV(
    '@L.PUBLIC.LINZ_STAGE/lds-world-7layers-SHP.zip',
    '@L.PUBLIC.LINZ_STAGE/csv/'
);

-- Verify CSVs are on stage
LIST @L.PUBLIC.LINZ_STAGE/csv/;

-- ============================================================
-- 5) COPY INTO SILVER tables
--    Column lists match SILVER table definitions exactly.
--    Add/remove $N positional refs once you see the actual CSV headers.
-- ============================================================

-- File format shared by all CSVs
CREATE OR REPLACE FILE FORMAT PUBLIC.LINZ_CSV
  TYPE             = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER      = 1
  NULL_IF          = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  DATE_FORMAT      = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';

-- These COPY INTO statements are templated — Snowflake will match
-- CSV columns positionally to $1, $2 … after the header is skipped.
-- Adjust the file pattern to the actual CSV name revealed in step 3.

COPY INTO SILVER.TITLE (
    TITLE_NO, LDT_LOC_ID, STATUS, ISSUE_DATE, REGISTER_TYPE,
    TYPE, AUDIT_ID, STE_ID, GUARANTEE_STATUS, PROVISIONAL,
    SUR_WRK_ID, MAORI_LAND, TTL_TITLE_NO_SRS, TTL_TITLE_NO_HEAD_SRS, IS_CURRENT
)
FROM (
    SELECT
        $1, $2::NUMBER, $3, TRY_TO_TIMESTAMP($4), $5,
        $6, $7::NUMBER, $8::NUMBER, $9, $10,
        $11::NUMBER, $12, $13, $14,
        IFF($3 IN ('LIVE','REGD'), TRUE, FALSE)
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*title.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.TITLE_ESTATE (
    ID, TTL_TITLE_NO, TYPE, STATUS, SHARE, PURPOSE,
    TIMESHARE_WEEK_NO, LGD_ID, ACT_TIN_ID_CRT, ORIGINAL_FLAG,
    TIN_ID_ORIG, TERM, ACT_ID_CRT, IS_CURRENT
)
FROM (
    SELECT
        $1::NUMBER, $2, $3, $4, $5, $6,
        $7, $8::NUMBER, $9::NUMBER, $10,
        $11::NUMBER, $12, $13::NUMBER,
        IFF($4 IN ('LIVE','REGD'), TRUE, FALSE)
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*title.estate.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.ESTATE_SHARE (
    ID, ETT_ID, STATUS, SHARE, ACT_TIN_ID_CRT,
    ORIGINAL_FLAG, SYSTEM_CRT, EXECUTORSHIP, ACT_ID_CRT, SHARE_MEMORIAL, IS_CURRENT
)
FROM (
    SELECT
        $1::NUMBER, $2::NUMBER, $3, $4, $5::NUMBER,
        $6, $7, $8, $9::NUMBER, $10,
        IFF($3 IN ('LIVE','REGD'), TRUE, FALSE)
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*estate.share.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.PROPRIETOR (
    ID, ETS_ID, STATUS, TYPE, PRIME_SURNAME,
    PRIME_OTHER_NAMES, NAME_SUFFIX, ORIGINAL_FLAG
)
FROM (
    SELECT
        $1::NUMBER, $2::NUMBER, $3, $4, $5,
        $6, $7, $8
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*proprietor.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.TITLE_INSTRUMENT (
    ID, DLG_ID, INST_NO, PRIORITY_NO, LDT_LOC_ID,
    LODGED_DATETIME, STATUS, TRT_GRP, TRT_TYPE, AUDIT_ID, TIN_ID_PARENT, IS_CURRENT
)
FROM (
    SELECT
        $1::NUMBER, $2::NUMBER, $3, $4::NUMBER, $5::NUMBER,
        TRY_TO_TIMESTAMP($6), $7, $8, $9, $10::NUMBER, $11::NUMBER,
        IFF($7 IN ('LIVE','REGD'), TRUE, FALSE)
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*title.instrument[^_title].*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.TITLE_INSTRUMENT_TITLE (TIN_ID, TTL_TITLE_NO, AUDIT_ID)
FROM (
    SELECT $1::NUMBER, $2, $3::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*title.instrument.title.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.ENCUMBRANCE (
    ID, STATUS, ACT_TIN_ID_CRT, ACT_TIN_ID_ORIG, ACT_ID_CRT, ACT_ID_ORIG, TERM, IS_CURRENT
)
FROM (
    SELECT
        $1::NUMBER, $2, $3::NUMBER, $4::NUMBER, $5::NUMBER, $6::NUMBER, $7,
        IFF($2 IN ('LIVE','REGD'), TRUE, FALSE)
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*encumbrance[^e].*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.ENCUMBRANCEE (ID, ENS_ID, STATUS, NAME)
FROM (
    SELECT $1::NUMBER, $2::NUMBER, $3, $4
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*encumbrancee.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.TITLE_ENCUMBRANCE (
    ID, TTL_TITLE_NO, ENC_ID, STATUS, ACT_TIN_ID_CRT, ACT_ID_CRT, IS_CURRENT
)
FROM (
    SELECT
        $1::NUMBER, $2, $3::NUMBER, $4, $5::NUMBER, $6::NUMBER,
        IFF($4 IN ('LIVE','REGD'), TRUE, FALSE)
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*title.encumbrance.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.TITLE_HIERARCHY (
    ID, STATUS, TTL_TITLE_NO_PRIOR, TTL_TITLE_NO_FLW, TDR_ID, ACT_TIN_ID_CRT, ACT_ID_CRT
)
FROM (
    SELECT $1::NUMBER, $2, $3, $4, $5::NUMBER, $6::NUMBER, $7::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*title.hierarchy.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.PARCEL (
    ID, LDT_LOC_ID, IMG_ID, FEN_ID, TOC_CODE, ALT_ID, AREA,
    NONSURVEY_DEF, APPELLATION_DATE, PARCEL_INTENT, STATUS,
    TOTAL_AREA, CALCULATED_AREA, SE_ROW_ID, AUDIT_ID
)
FROM (
    SELECT
        $1::NUMBER, $2::NUMBER, $3::NUMBER, $4::NUMBER, $5, $6::NUMBER,
        $7::NUMBER(20,4), $8, TRY_TO_TIMESTAMP($9), $10, $11,
        $12::NUMBER(20,4), $13::NUMBER(20,4), $14::NUMBER, $15::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*parcel[^_].*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.APPELLATION (
    ID, PAR_ID, TYPE, TITLE_FLAG, SURVEY_FLAG, STATUS, PART_INDICATOR,
    MAORI_NAME, SUB_TYPE, APPELLATION_VALUE, PARCEL_TYPE, PARCEL_VALUE,
    SECOND_PARCEL_TYPE, SECOND_PRCL_VALUE, BLOCK_NUMBER, SUB_TYPE_POSITION, ACT_ID_CRT, AUDIT_ID
)
FROM (
    SELECT
        $1::NUMBER, $2::NUMBER, $3, $4, $5, $6, $7,
        $8, $9, $10, $11, $12,
        $13, $14, $15, $16, $17::NUMBER, $18::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*appellation.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.LAND_DISTRICT (
    LDT_LOC_ID, OFF_CODE, NAME, ABBREV, STATUS, DEFAULT_IND, USR_TM_ID, AUDIT_ID
)
FROM (
    SELECT $1::NUMBER, $2, $3, $4, $5, $6, $7, $8::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*land.district.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.TRANSACTION_TYPE (GRP, TYPE, DESCRIPTION, AUDIT_ID)
FROM (
    SELECT $1, $2, $3, $4::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*transaction.type.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.LEGAL_DESCRIPTION (
    ID, TYPE, LOT_NUMBER, DEPOSITED_PLAN_NUMBER, FLAT_PLAN_NUMBER, STATUS, AUDIT_ID
)
FROM (
    SELECT $1::NUMBER, $2, $3, $4, $5, $6, $7::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*legal.description[^_].*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.LEGAL_DESCRIPTION_PARCEL (LEG_ID, PAR_ID, AUDIT_ID)
FROM (
    SELECT $1::NUMBER, $2::NUMBER, $3::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*legal.description.parcel.*\\.csv')
)
ON_ERROR = CONTINUE;

COPY INTO SILVER.TITLE_PARCEL_ASSOCIATION (TTL_TITLE_NO, PAR_ID, STATUS, AUDIT_ID)
FROM (
    SELECT $1, $2::NUMBER, $3, $4::NUMBER
    FROM @L.PUBLIC.LINZ_STAGE/csv/
    (FILE_FORMAT => PUBLIC.LINZ_CSV, PATTERN => '.*title.parcel.*\\.csv')
)
ON_ERROR = CONTINUE;

-- ============================================================
-- 6) Row count smoke check across all SILVER tables
-- ============================================================
SELECT 'TITLE'                    AS ENTITY, COUNT(*) AS ROWS FROM SILVER.TITLE                    UNION ALL
SELECT 'TITLE_ESTATE',                       COUNT(*) FROM SILVER.TITLE_ESTATE                    UNION ALL
SELECT 'ESTATE_SHARE',                       COUNT(*) FROM SILVER.ESTATE_SHARE                   UNION ALL
SELECT 'PROPRIETOR',                         COUNT(*) FROM SILVER.PROPRIETOR                     UNION ALL
SELECT 'TITLE_INSTRUMENT',                   COUNT(*) FROM SILVER.TITLE_INSTRUMENT               UNION ALL
SELECT 'TITLE_INSTRUMENT_TITLE',             COUNT(*) FROM SILVER.TITLE_INSTRUMENT_TITLE         UNION ALL
SELECT 'ENCUMBRANCE',                        COUNT(*) FROM SILVER.ENCUMBRANCE                    UNION ALL
SELECT 'ENCUMBRANCEE',                       COUNT(*) FROM SILVER.ENCUMBRANCEE                   UNION ALL
SELECT 'TITLE_ENCUMBRANCE',                  COUNT(*) FROM SILVER.TITLE_ENCUMBRANCE              UNION ALL
SELECT 'TITLE_HIERARCHY',                    COUNT(*) FROM SILVER.TITLE_HIERARCHY                UNION ALL
SELECT 'PARCEL',                             COUNT(*) FROM SILVER.PARCEL                         UNION ALL
SELECT 'APPELLATION',                        COUNT(*) FROM SILVER.APPELLATION                    UNION ALL
SELECT 'LAND_DISTRICT',                      COUNT(*) FROM SILVER.LAND_DISTRICT                  UNION ALL
SELECT 'TRANSACTION_TYPE',                   COUNT(*) FROM SILVER.TRANSACTION_TYPE               UNION ALL
SELECT 'LEGAL_DESCRIPTION',                  COUNT(*) FROM SILVER.LEGAL_DESCRIPTION              UNION ALL
SELECT 'LEGAL_DESCRIPTION_PARCEL',           COUNT(*) FROM SILVER.LEGAL_DESCRIPTION_PARCEL       UNION ALL
SELECT 'TITLE_PARCEL_ASSOCIATION',           COUNT(*) FROM SILVER.TITLE_PARCEL_ASSOCIATION
ORDER BY ROWS DESC;
