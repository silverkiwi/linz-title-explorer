"""
Download missing LINZ tables from LDS API → Snowflake stage → SILVER tables.
Tables: PROPRIETOR, ESTATE_SHARE, ENCUMBRANCEE, PARCEL (attrs), LAND_DISTRICT (attrs)
"""

import os, sys, time, zipfile, io, requests, subprocess, tempfile

API_KEY  = os.environ.get("LINZ_API_KEY", "43760376f52e40dbaa4d56e7d7bf6bd2")
BASE_URL = "https://data.linz.govt.nz/services/api/v1"
HEADERS  = {"Authorization": f"key {API_KEY}", "Content-Type": "application/json"}

SNOWSQL  = os.path.expanduser("~/.snowsql/1.2.28/snowsql")
SF_ARGS  = ["-a", "LTLREQB-LU08831", "-u", "LINZ", "-d", "L", "-s", "PUBLIC", "-w", "COMPUTE_WH"]
SF_ENV   = {**os.environ, "SNOWSQL_PWD": "Banana123!!!!!!"}

# Tables to fetch: (label, item_url, stage_prefix, SILVER_table, col_map)
# col_map: CSV header → SILVER column (only needed where names differ)
TARGETS = [
    {
        "label":       "PROPRIETOR",
        "item_url":    f"{BASE_URL}/tables/51998/",
        "silver":      "SILVER.PROPRIETOR",
        "col_map":     {},   # CSV headers match SILVER columns
    },
    {
        "label":       "ESTATE_SHARE",
        "item_url":    f"{BASE_URL}/tables/52065/",
        "silver":      "SILVER.ESTATE_SHARE",
        "col_map":     {},
    },
    {
        "label":       "ENCUMBRANCEE",
        "item_url":    f"{BASE_URL}/tables/51985/",
        "silver":      "SILVER.ENCUMBRANCEE",
        "col_map":     {},
    },
    {
        "label":       "PARCEL",
        "item_url":    f"{BASE_URL}/layers/51976/",
        "silver":      "SILVER.PARCEL",
        "col_map":     {},
    },
    {
        "label":       "LAND_DISTRICT",
        "item_url":    f"{BASE_URL}/layers/52070/",
        "silver":      "SILVER.LAND_DISTRICT",
        "col_map":     {},
    },
]


def create_export(item_url: str) -> dict:
    payload = {
        "crs": "EPSG:4326",
        "formats": {"vector": "text/csv"},
        "items": [{"item": item_url}],
    }
    r = requests.post(f"{BASE_URL}/exports/", json=payload, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def poll_export(export_id: int, label: str) -> str:
    """Poll until export is complete; return download URL."""
    url = f"{BASE_URL}/exports/{export_id}/"
    for attempt in range(120):   # up to ~20 min
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data   = r.json()
        state  = data.get("state", "")
        print(f"  [{label}] state={state} ({attempt*10}s elapsed)", flush=True)
        if state == "complete":
            return data["download_url"]
        if state in ("error", "cancelled"):
            raise RuntimeError(f"Export {export_id} failed: {data}")
        time.sleep(10)
    raise TimeoutError(f"Export {export_id} timed out")


def download_file(download_url: str, label: str) -> bytes:
    print(f"  [{label}] Downloading …", flush=True)
    r = requests.get(download_url, headers=HEADERS, stream=True)
    r.raise_for_status()
    buf = io.BytesIO()
    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
        buf.write(chunk)
    return buf.getvalue()


def extract_csv(zip_bytes: bytes, label: str) -> tuple[str, bytes]:
    """Return (csv_filename, csv_bytes) from the export ZIP."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise ValueError(f"[{label}] No CSV found in export ZIP. Files: {zf.namelist()}")
        # Pick the largest CSV (skip metadata files)
        name = max(csvs, key=lambda n: zf.getinfo(n).file_size)
        print(f"  [{label}] Extracting {name} ({zf.getinfo(name).file_size:,} bytes uncompressed)", flush=True)
        return name.split("/")[-1], zf.read(name)


def snowsql(sql: str, label: str):
    result = subprocess.run(
        [SNOWSQL, *SF_ARGS, "-q", sql],
        env=SF_ENV, capture_output=True, text=True
    )
    if result.returncode not in (0, 1):   # snowsql returns 1 on non-fatal warnings
        print(f"  [{label}] snowsql stderr: {result.stderr[:500]}", flush=True)
    print(result.stdout[-800:], flush=True)


def put_and_load(csv_name: str, csv_bytes: bytes, target: dict):
    label  = target["label"]
    silver = target["silver"]

    # Write CSV to a temp file, then PUT to stage
    with tempfile.NamedTemporaryFile(suffix=f"_{csv_name}", delete=False) as tmp:
        tmp.write(csv_bytes)
        tmp_path = tmp.name

    stage_path = f"@L.PUBLIC.LINZ_STAGE/missing/{csv_name}"
    put_sql = f"PUT 'file://{tmp_path}' '@L.PUBLIC.LINZ_STAGE/missing/' OVERWRITE=TRUE AUTO_COMPRESS=FALSE;"
    print(f"  [{label}] PUT → stage …", flush=True)
    snowsql(put_sql, label)
    os.unlink(tmp_path)

    # Inspect first line to get actual CSV headers
    first_line = csv_bytes.split(b"\n")[0].decode("utf-8", errors="replace").strip()
    raw_cols   = [c.strip('"') for c in first_line.split(",")]
    col_map    = target.get("col_map", {})
    silver_cols = [col_map.get(c, c.upper()) for c in raw_cols]

    # Build COPY INTO with column list
    col_list   = ", ".join(silver_cols)
    positional = ", ".join(f"${i+1}" for i in range(len(raw_cols)))

    copy_sql = f"""
CREATE OR REPLACE FILE FORMAT PUBLIC.LINZ_CSV_SIMPLE
  TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO';

TRUNCATE TABLE {silver};

COPY INTO {silver} ({col_list})
FROM (SELECT {positional} FROM {stage_path} (FILE_FORMAT => PUBLIC.LINZ_CSV_SIMPLE))
ON_ERROR = CONTINUE;

SELECT '{label}' AS entity, COUNT(*) AS rows FROM {silver};
"""
    print(f"  [{label}] COPY INTO {silver} …", flush=True)
    snowsql(copy_sql, label)


def main():
    print("=== Submitting LDS export jobs ===", flush=True)
    jobs = []
    for t in TARGETS:
        try:
            export = create_export(t["item_url"])
            export_id = export["id"]
            print(f"  [{t['label']}] export job {export_id} created", flush=True)
            jobs.append({**t, "export_id": export_id})
        except Exception as e:
            print(f"  [{t['label']}] FAILED to create export: {e}", flush=True)

    print("\n=== Polling export jobs ===", flush=True)
    for job in jobs:
        label = job["label"]
        try:
            dl_url   = poll_export(job["export_id"], label)
            zip_data = download_file(dl_url, label)
            csv_name, csv_bytes = extract_csv(zip_data, label)
            put_and_load(csv_name, csv_bytes, job)
        except Exception as e:
            print(f"  [{label}] ERROR: {e}", flush=True)

    print("\n=== Done ===", flush=True)


if __name__ == "__main__":
    main()
