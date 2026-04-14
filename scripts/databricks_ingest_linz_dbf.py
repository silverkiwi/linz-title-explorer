#!/usr/bin/env python3
"""
Chunked DBF -> Delta ingester for the 10-layer LINZ Landonline package.

Intended to run in Databricks on a cluster with:
- PySpark available
- dbfread installed (`%pip install dbfread`)
- DBF files accessible from DBFS/Volumes/local disk

Example:
  python scripts/databricks_ingest_linz_dbf.py \
    --source-dir /dbfs/FileStore/linz/lds-world-10layers-unzipped \
    --catalog main \
    --bronze-schema bronze \
    --extract-date 2026-04-06
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import date, datetime
from typing import Dict, Iterable, Iterator, List

from dbfread import DBF
from pyspark.sql import SparkSession
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True, help="Directory holding unzipped layer folders")
    parser.add_argument("--catalog", default=None, help="Optional Unity Catalog catalog name")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--extract-date", default=str(date.today()))
    parser.add_argument("--chunk-size", type=int, default=50000)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    return parser.parse_args()


def qualify(catalog: str | None, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"


def chunks(records: Iterable[dict], size: int) -> Iterator[List[dict]]:
    batch: List[dict] = []
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


def ingest_table(
    spark: SparkSession,
    layer_dir: str,
    source_table: str,
    target_table: str,
    extract_date: str,
    chunk_size: int,
    mode: str,
) -> None:
    dbf_path = os.path.join(layer_dir, f"{source_table}.dbf")
    if not os.path.exists(dbf_path):
        raise FileNotFoundError(dbf_path)

    batch_id = str(uuid.uuid4())
    table = DBF(dbf_path, load=False, char_decode_errors="ignore")

    first = True
    total_rows = 0
    started_at = datetime.utcnow()

    for batch in chunks((normalize_record(r) for r in table), chunk_size):
        df = spark.createDataFrame(batch)
        df = (
            df.withColumn("source_file", F.lit(dbf_path))
            .withColumn("source_table", F.lit(source_table))
            .withColumn("source_extract_date", F.to_date(F.lit(extract_date)))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("ingest_batch_id", F.lit(batch_id))
        )

        write_mode = mode if first else "append"
        (
            df.write.format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true" if write_mode == "overwrite" else "false")
            .saveAsTable(target_table)
        )
        total_rows += len(batch)
        first = False
        print(f"[{source_table}] wrote {total_rows:,} rows")

    ended_at = datetime.utcnow()
    print(f"[{source_table}] completed: {total_rows:,} rows in {(ended_at - started_at).total_seconds():.1f}s")


def ensure_schemas(spark: SparkSession, catalog: str | None, bronze_schema: str) -> None:
    if catalog:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
        spark.sql("CREATE SCHEMA IF NOT EXISTS ops")


def write_audit_counts(
    spark: SparkSession,
    catalog: str | None,
    bronze_schema: str,
    extract_date: str,
) -> None:
    rows = []
    for _, target_name in TABLES.items():
        full_name = qualify(catalog, bronze_schema, target_name)
        count = spark.table(full_name).count()
        rows.append({
            "table_name": full_name,
            "row_count": count,
            "extract_date": extract_date,
            "recorded_at": datetime.utcnow(),
        })
    audit_df = spark.createDataFrame(rows)
    audit_target = qualify(catalog, "ops", "linz_table_counts")
    audit_df.write.format("delta").mode("append").saveAsTable(audit_target)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("linz-dbf-ingest").getOrCreate()

    ensure_schemas(spark, args.catalog, args.bronze_schema)

    for source_table, target_name in TABLES.items():
        layer_dir = os.path.join(args.source_dir, source_table)
        target_table = qualify(args.catalog, args.bronze_schema, target_name)
        ingest_table(
            spark=spark,
            layer_dir=layer_dir,
            source_table=source_table,
            target_table=target_table,
            extract_date=args.extract_date,
            chunk_size=args.chunk_size,
            mode=args.mode,
        )

    write_audit_counts(
        spark=spark,
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        extract_date=args.extract_date,
    )


if __name__ == "__main__":
    main()
