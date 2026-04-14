"""
Load AIMS address DBF files from local disk directly into Snowflake.
Files: aims-address, aims-address-component (×2 split)
"""

import os, struct, sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

SNOWFLAKE = dict(
    account   = "LTLREQB-LU08831",
    user      = "LINZ",
    password  = "Banana123!!!!!!",
    database  = "L",
    schema    = "SILVER",
    warehouse = "COMPUTE_WH",
)

BASE = os.path.expanduser(
    "~/linz/lds-world-14layers-SHP"
)

NULL_CHARS = {' ', '*', '\x00'}
BATCH = 50_000


def parse_val(raw, ftype):
    v = raw.decode('utf-8', errors='replace').strip()
    if not v or all(c in NULL_CHARS for c in v):
        return None
    if ftype in ('N', 'F'):
        try:
            return float(v) if '.' in v else int(v)
        except ValueError:
            return None
    if ftype == 'D':
        return f"{v[:4]}-{v[4:6]}-{v[6:8]}" if len(v) == 8 and v.isdigit() else None
    return v


def stream_dbf(path):
    """Yield (col_names, batch_list) for each batch in a DBF file."""
    with open(path, 'rb') as f:
        hdr        = f.read(32)
        num_recs   = struct.unpack_from('<I', hdr, 4)[0]
        hdr_size   = struct.unpack_from('<H', hdr, 8)[0]
        rec_size   = struct.unpack_from('<H', hdr, 10)[0]
        rest       = f.read(hdr_size - 32)

        fields, pos = [], 0
        while pos < len(rest) - 1 and rest[pos:pos+1] != b'\r':
            name   = rest[pos:pos+11].rstrip(b'\x00').decode('utf-8', errors='replace')
            ftype  = chr(rest[pos + 11])
            length = rest[pos + 16]
            fields.append((name, ftype, length))
            pos += 32

        cols  = [f[0].upper() for f in fields]
        batch = []
        for _ in range(num_recs):
            rec = f.read(rec_size)
            if not rec or len(rec) < rec_size or rec[0:1] == b'\x1a':
                break
            if rec[0:1] != b'*':
                vals, off = [], 1
                for _, ftype, length in fields:
                    vals.append(parse_val(rec[off:off+length], ftype))
                    off += length
                batch.append(vals)
                if len(batch) >= BATCH:
                    yield cols, batch
                    batch = []
        if batch:
            yield cols, batch


def load_file(conn, path, table, col_rename: dict, label: str, drop_cols: list = None):
    total = 0
    for cols, batch in stream_dbf(path):
        renamed = [col_rename.get(c, c) for c in cols]
        pdf = pd.DataFrame(batch, columns=renamed)
        if drop_cols:
            pdf.drop(columns=[c for c in drop_cols if c in pdf.columns], inplace=True)
        success, nchunks, nrows, _ = write_pandas(
            conn, pdf, table,
            database='L', schema='SILVER',
            overwrite=False, auto_create_table=False,
            quote_identifiers=False,
        )
        total += nrows
        print(f"  [{label}] {total:,} rows written", flush=True)
    print(f"  [{label}] DONE — {total:,} total rows", flush=True)
    return total


def run_sql_file(path: str):
    """Run a SQL file via snowsql to create tables/views."""
    import subprocess
    env = {**os.environ, "SNOWSQL_PWD": "Banana123!!!!!!"}
    snowsql = os.path.expanduser("~/.snowsql/1.2.28/snowsql")
    args = [snowsql, "-a", "LTLREQB-LU08831", "-u", "LINZ",
            "-d", "L", "-s", "PUBLIC", "-w", "COMPUTE_WH", "-f", path]
    result = subprocess.run(args, env=env, capture_output=True, text=True)
    print(result.stdout[-500:], flush=True)


def main():
    print("=== Creating tables + views ===", flush=True)
    run_sql_file(os.path.expanduser("~/linz/sql/14_aims_address.sql"))

    print("Connecting to Snowflake…", flush=True)
    conn = snowflake.connector.connect(**SNOWFLAKE)

    # -- AIMS_ADDRESS --
    print("\n=== AIMS_ADDRESS ===", flush=True)
    conn.cursor().execute("TRUNCATE TABLE SILVER.AIMS_ADDRESS")
    load_file(
        conn,
        f"{BASE}/aims-address-deprecated/aims-address-deprecated.dbf",
        "AIMS_ADDRESS",
        col_rename={
            "ADDRESS_ID":  "ADDRESS_ID",
            "CHANGE_ID":   "CHANGE_ID",
            "PRIMARY_AD":  "PRIMARY_AD",
            "ADDRESS_LI":  "STATUS",
            "ADDRESS_PR":  "PRODUCER",
            "ADDRESS_MA":  "MAINTAINER",
            "ADDRESSABL":  "ADDRESSABLE_ID",
            "ADDRESS_CL":  "ADDRESS_CLASS",
            "PARCEL_ID":   "PARCEL_ID",
        },
        label="AIMS_ADDRESS",
        drop_cols=["CHANGE_ID", "PRIMARY_AD"],
    )

    # -- AIMS_ADDRESS_COMPONENT (2 split files) --
    print("\n=== AIMS_ADDRESS_COMPONENT ===", flush=True)
    conn.cursor().execute("TRUNCATE TABLE SILVER.AIMS_ADDRESS_COMPONENT")
    comp_rename = {
        "ADDRESS_CO":  "COMPONENT_ID",
        "ADDRESS_ID":  "ADDRESS_ID",
        "ADDRESS__1":  "COMPONENT_TYPE",
        "ADDRESS__2":  "COMPONENT_VALUE",
        "ADDRESS__3":  "COMPONENT_SEQ",
        "ADDRESS__4":  "COMPONENT_GROUP",
        "BEGIN_LIFE":  "BEGIN_LIFECYCLE",
        "END_LIFESP":  "END_LIFECYCLE",
    }
    for suffix, label in [("", "COMPONENT part 1"), (".2", "COMPONENT part 2")]:
        path = f"{BASE}/aims-address-component-deprecated/aims-address-component-deprecated{suffix}.dbf"
        if os.path.exists(path):
            load_file(conn, path, "AIMS_ADDRESS_COMPONENT", comp_rename, label)

    conn.close()

    print("\n=== Smoke check ===", flush=True)
    conn2 = snowflake.connector.connect(**SNOWFLAKE)
    cur = conn2.cursor()
    cur.execute("SELECT COUNT(*) FROM SILVER.AIMS_ADDRESS")
    print(f"  AIMS_ADDRESS:          {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM SILVER.AIMS_ADDRESS_COMPONENT")
    print(f"  AIMS_ADDRESS_COMPONENT:{cur.fetchone()[0]:,}")
    cur.execute("""
        SELECT COUNT(*) FROM GOLD.DIM_ADDRESS
        WHERE title_no IS NOT NULL
    """)
    print(f"  DIM_ADDRESS (with title): {cur.fetchone()[0]:,}")
    cur.execute("""
        SELECT title_no, full_address
        FROM GOLD.DIM_ADDRESS
        WHERE title_no IS NOT NULL
          AND full_address IS NOT NULL
        LIMIT 5
    """)
    print("\n  Sample addresses:")
    for row in cur:
        print(f"    {row[0]:20s}  {row[1]}")
    conn2.close()
    print("\nAll done!", flush=True)


if __name__ == "__main__":
    main()
