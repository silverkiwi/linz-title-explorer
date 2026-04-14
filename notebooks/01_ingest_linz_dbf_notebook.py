# Databricks notebook source
# MAGIC %pip install dbfread

# COMMAND ----------

# CONFIG
CATALOG = "main"            # set to None if not using Unity Catalog
BRONZE_SCHEMA = "bronze"
OPS_SCHEMA = "ops"
SOURCE_DIR = "/dbfs/FileStore/linz/lds-world-10layers-unzipped"  # folder containing the 10 extracted layer folders
EXTRACT_DATE = "2026-04-06"  # adjust to your package date
CHUNK_SIZE = 50000
WRITE_MODE = "overwrite"     # overwrite or append

# COMMAND ----------

from __future__ import annotations

import os
import uuid
from datetime import datetime
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


def ingest_table(source_table: str, target_name: str):
    layer_dir = os.path.join(SOURCE_DIR, source_table)
    dbf_path = os.path.join(layer_dir, f"{source_table}.dbf")
    if not os.path.exists(dbf_path):
        raise FileNotFoundError(f"Missing DBF: {dbf_path}")

    target_table = qualify(CATALOG, BRONZE_SCHEMA, target_name)
    batch_id = str(uuid.uuid4())
    table = DBF(dbf_path, load=False, char_decode_errors="ignore")

    total_rows = 0
    first = True
    started_at = datetime.utcnow()

    for batch in chunks((normalize_record(r) for r in table), CHUNK_SIZE):
        df = spark.createDataFrame(batch)
        df = (
            df.withColumn("source_file", F.lit(dbf_path))
              .withColumn("source_table", F.lit(source_table))
              .withColumn("source_extract_date", F.to_date(F.lit(EXTRACT_DATE)))
              .withColumn("ingested_at", F.current_timestamp())
              .withColumn("ingest_batch_id", F.lit(batch_id))
        )

        write_mode = WRITE_MODE if first else "append"
        (
            df.write.format("delta")
              .mode(write_mode)
              .option("overwriteSchema", "true" if write_mode == "overwrite" else "false")
              .saveAsTable(target_table)
        )
        total_rows += len(batch)
        first = False
        print(f"[{source_table}] wrote {total_rows:,} rows -> {target_table}")

    elapsed = (datetime.utcnow() - started_at).total_seconds()
    print(f"[{source_table}] completed {total_rows:,} rows in {elapsed:.1f}s")


def write_audit_counts():
    rows = []
    for _, target_name in TABLES.items():
        full_name = qualify(CATALOG, BRONZE_SCHEMA, target_name)
        count = spark.table(full_name).count()
        rows.append({
            "table_name": full_name,
            "row_count": count,
            "extract_date": EXTRACT_DATE,
            "recorded_at": datetime.utcnow(),
        })

    audit_df = spark.createDataFrame(rows)
    audit_target = qualify(CATALOG, OPS_SCHEMA, "linz_table_counts")
    audit_df.write.format("delta").mode("append").saveAsTable(audit_target)
    print(f"Wrote audit counts -> {audit_target}")

# COMMAND ----------

# Pre-flight checks
ensure_schemas()

missing = []
for source_table in TABLES:
    dbf_path = os.path.join(SOURCE_DIR, source_table, f"{source_table}.dbf")
    if not os.path.exists(dbf_path):
        missing.append(dbf_path)

if missing:
    raise Exception("Missing expected DBF files:\n" + "\n".join(missing))

print("All expected DBF files found.")
display(spark.createDataFrame([(k, qualify(CATALOG, BRONZE_SCHEMA, v)) for k, v in TABLES.items()], ["source_table", "target_table"]))

# COMMAND ----------

# Run ingest for all 10 tables
for source_table, target_name in TABLES.items():
    ingest_table(source_table, target_name)

write_audit_counts()

# COMMAND ----------

# Quick verification
verification_tables = [qualify(CATALOG, BRONZE_SCHEMA, t) for t in TABLES.values()]
rows = [(t, spark.table(t).count()) for t in verification_tables]
display(spark.createDataFrame(rows, ["table_name", "row_count"]))
