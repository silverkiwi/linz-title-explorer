# Databricks notebook source
# MAGIC %pip install dbfread

# COMMAND ----------

# After running the %pip cell above, use Databricks "Restart Python" before continuing.

# CONFIG
CATALOG = "main"
BRONZE_SCHEMA = "bronze"
OPS_SCHEMA = "ops"
SOURCE_DIR = "/Volumes/main/default/linz_data/lds-world-10layers-unzipped"
EXTRACT_DATE = "2026-04-06"
CHUNK_SIZE = 10000
WRITE_MODE = "overwrite"   # overwrite for first test, append for reruns if desired
MAX_WRITE_RETRIES = 3

# Smallest / safest test order first
TEST_TABLES = [
    "landonline-transaction-type",
    "landonline-title-document-reference",
]

ALL_TABLES = [
    "landonline-transaction-type",
    "landonline-title-document-reference",
    "landonline-title",
    "landonline-title-estate",
    "landonline-title-hierarchy",
    "landonline-encumbrance",
    "landonline-encumbrance-share",
    "landonline-title-encumbrance",
    "landonline-title-instrument",
    "landonline-title-instrument-title",
]

# COMMAND ----------

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime
from typing import Iterable, Iterator, List

from dbfread import DBF
from pyspark.sql import functions as F

TABLES = {
    "landonline-title": "linz_title_raw",
    "landonline-title-estate": "linz_title_estate_raw",
    "landonline-title-instrument": "linz_title_instrument_raw",
    "landonline-title-instrument-title": "linz_title_instrument_title_raw",
    "landonline-title-hierarchy": "linz_title_hierarchy_raw",
    "landonline-title-encumbrance": "linz_title_encumbrance_raw",
    "landonline-encumbrance": "linz_encumbrance_raw",
    "landonline-encumbrance-share": "linz_encumbrance_share_raw",
    "landonline-title-document-reference": "linz_title_document_reference_raw",
    "landonline-transaction-type": "linz_transaction_type_raw",
}


def qualify(catalog, schema, table):
    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"


def chunks(records: Iterable[dict], size: int) -> Iterator[List[dict]]:
    batch = []
    for record in records:
        batch.append(record)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def normalize_record(record: dict) -> dict:
    out = {}
    for key, value in record.items():
        if isinstance(value, str):
            value = value.strip()
        out[key.lower()] = value
    return out


def ensure_schemas():
    if CATALOG:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OPS_SCHEMA}")
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA}")


def table_dbf_path(source_table: str) -> str:
    return os.path.join(SOURCE_DIR, source_table, f"{source_table}.dbf")


def preflight_check(source_tables: List[str]):
    ensure_schemas()
    missing = []
    for source_table in source_tables:
        dbf_path = table_dbf_path(source_table)
        if not os.path.exists(dbf_path):
            missing.append(dbf_path)
    if missing:
        raise Exception("Missing expected DBF files:\n" + "\n".join(missing))

    spark.createDataFrame(
        [(k, qualify(CATALOG, BRONZE_SCHEMA, TABLES[k]), table_dbf_path(k)) for k in source_tables],
        ["source_table", "target_table", "dbf_path"],
    ).show(truncate=False)


def write_batch_with_retry(df, target_table: str, write_mode: str):
    last_error = None
    for attempt in range(1, MAX_WRITE_RETRIES + 1):
        try:
            (
                df.write.format("delta")
                .mode(write_mode)
                .option("overwriteSchema", "true" if write_mode == "overwrite" else "false")
                .saveAsTable(target_table)
            )
            return
        except Exception as e:
            last_error = e
            print(f"Write failed to {target_table} on attempt {attempt}/{MAX_WRITE_RETRIES}: {e}")
            if attempt < MAX_WRITE_RETRIES:
                sleep_seconds = attempt * 10
                print(f"Retrying in {sleep_seconds}s...")
                time.sleep(sleep_seconds)
    raise last_error


def ingest_table(source_table: str, overwrite_table: bool | None = None):
    target_name = TABLES[source_table]
    target_table = qualify(CATALOG, BRONZE_SCHEMA, target_name)
    dbf_path = table_dbf_path(source_table)

    if not os.path.exists(dbf_path):
        raise FileNotFoundError(f"Missing DBF: {dbf_path}")

    if overwrite_table is None:
        overwrite_table = WRITE_MODE == "overwrite"

    batch_id = str(uuid.uuid4())
    table = DBF(dbf_path, load=False, char_decode_errors="ignore")

    total_rows = 0
    first = True
    started_at = datetime.now(UTC)

    for batch_number, batch in enumerate(chunks((normalize_record(r) for r in table), CHUNK_SIZE), start=1):
        df = spark.createDataFrame(batch)
        df = (
            df.withColumn("source_file", F.lit(dbf_path))
              .withColumn("source_table", F.lit(source_table))
              .withColumn("source_extract_date", F.to_date(F.lit(EXTRACT_DATE)))
              .withColumn("ingested_at", F.current_timestamp())
              .withColumn("ingest_batch_id", F.lit(batch_id))
        )

        if first and overwrite_table:
            mode = "overwrite"
        else:
            mode = "append"

        write_batch_with_retry(df, target_table, mode)

        total_rows += len(batch)
        first = False
        print(f"[{source_table}] batch {batch_number} wrote {len(batch):,} rows | total {total_rows:,} -> {target_table}")

    elapsed = (datetime.now(UTC) - started_at).total_seconds()
    print(f"[{source_table}] completed {total_rows:,} rows in {elapsed:.1f}s")


def verify_table(source_table: str):
    target_table = qualify(CATALOG, BRONZE_SCHEMA, TABLES[source_table])
    count = spark.table(target_table).count()
    print(f"Verified {target_table}: {count:,} rows")


def write_audit_counts(source_tables: List[str]):
    rows = []
    for source_table in source_tables:
        target_name = TABLES[source_table]
        full_name = qualify(CATALOG, BRONZE_SCHEMA, target_name)
        count = spark.table(full_name).count()
        rows.append({
            "table_name": full_name,
            "row_count": count,
            "extract_date": EXTRACT_DATE,
            "recorded_at": datetime.now(UTC),
        })

    audit_df = spark.createDataFrame(rows)
    audit_target = qualify(CATALOG, OPS_SCHEMA, "linz_table_counts")
    audit_df.write.format("delta").mode("append").saveAsTable(audit_target)
    print(f"Wrote audit counts -> {audit_target}")

# COMMAND ----------

# 1) Preflight only on the smallest test tables first
preflight_check(TEST_TABLES)

# COMMAND ----------

# 2) Run a tiny first test
for source_table in TEST_TABLES:
    ingest_table(source_table, overwrite_table=True)
    verify_table(source_table)

write_audit_counts(TEST_TABLES)

# COMMAND ----------

# 3) If the small-table test succeeds, run tables one at a time from here.
# Uncomment one line at a time.

# preflight_check(["landonline-title"])
# ingest_table("landonline-title", overwrite_table=True)
# verify_table("landonline-title")

# preflight_check(["landonline-title-estate"])
# ingest_table("landonline-title-estate", overwrite_table=True)
# verify_table("landonline-title-estate")

# preflight_check(["landonline-title-hierarchy"])
# ingest_table("landonline-title-hierarchy", overwrite_table=True)
# verify_table("landonline-title-hierarchy")

# preflight_check(["landonline-encumbrance"])
# ingest_table("landonline-encumbrance", overwrite_table=True)
# verify_table("landonline-encumbrance")

# preflight_check(["landonline-encumbrance-share"])
# ingest_table("landonline-encumbrance-share", overwrite_table=True)
# verify_table("landonline-encumbrance-share")

# preflight_check(["landonline-title-encumbrance"])
# ingest_table("landonline-title-encumbrance", overwrite_table=True)
# verify_table("landonline-title-encumbrance")

# preflight_check(["landonline-title-instrument"])
# ingest_table("landonline-title-instrument", overwrite_table=True)
# verify_table("landonline-title-instrument")

# preflight_check(["landonline-title-instrument-title"])
# ingest_table("landonline-title-instrument-title", overwrite_table=True)
# verify_table("landonline-title-instrument-title")
