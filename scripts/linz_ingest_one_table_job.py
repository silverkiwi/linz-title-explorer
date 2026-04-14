#!/usr/bin/env python3
"""
Databricks job runner: ingest ONE LINZ DBF table -> main.bronze.*

Use one job task per source table for max reliability.

Job parameters (recommended):
- source_table (required)
- source_dir (default: /Volumes/main/default/linz_data/lds-world-10layers-unzipped)
- extract_date (default: 2026-04-06)
- catalog (default: main)
- bronze_schema (default: bronze)
- ops_schema (default: ops)
- chunk_size (optional; if omitted uses table defaults)
- overwrite (default: true)
"""

from __future__ import annotations

import argparse
import glob
import os
import time
import uuid
from datetime import UTC, date, datetime
from typing import Iterable, Iterator, List

from dbfread import DBF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

TABLE_TO_TARGET = {
    # Original 10 title/instrument layers
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
    # 7-layer additions
    "landonline-appellation": "linz_appellation_raw",
    "landonline-legal-description": "linz_legal_description_raw",
    "landonline-legal-description-parcel": "linz_legal_description_parcel_raw",
    "landonline-statute": "linz_statute_raw",
    "landonline-statute-action": "linz_statute_action_raw",
    "landonline-statutory-action-parcel": "linz_statutory_action_parcel_raw",
    "landonline-title-parcel-association": "linz_title_parcel_association_raw",
    # 14-layer AIMS deprecated additions
    "aims-address-class-deprecated": "aims_address_class_deprecated_raw",
    "aims-address-component-deprecated": "aims_address_component_deprecated_raw",
    "aims-address-component-type-deprecated": "aims_address_component_type_deprecated_raw",
    "aims-address-deprecated": "aims_address_deprecated_raw",
    "aims-address-lifecycle-stage-deprecated": "aims_address_lifecycle_stage_deprecated_raw",
    "aims-address-position-type-deprecated": "aims_address_position_type_deprecated_raw",
    "aims-address-reference-deprecated": "aims_address_reference_deprecated_raw",
    "aims-address-reference-object-type-deprecated": "aims_address_reference_object_type_deprecated_raw",
    "aims-addressable-object-deprecated": "aims_addressable_object_deprecated_raw",
    "aims-addressable-object-external-deprecated": "aims_addressable_object_external_deprecated_raw",
    "aims-addressable-object-lifecycle-stage-deprecated": "aims_addressable_object_lifecycle_stage_deprecated_raw",
    "aims-addressable-object-type-deprecated": "aims_addressable_object_type_deprecated_raw",
    "aims-alternative-address-type-deprecated": "aims_alternative_address_type_deprecated_raw",
    "aims-organisation-deprecated": "aims_organisation_deprecated_raw",
}

DEFAULT_CHUNK_BY_TABLE = {
    "landonline-transaction-type": 20000,
    "landonline-title-document-reference": 10000,
    "landonline-title": 5000,
    "landonline-title-hierarchy": 5000,
    "landonline-encumbrance": 5000,
    "landonline-encumbrance-share": 5000,
    "landonline-title-encumbrance": 5000,
    "landonline-title-estate": 3000,
    "landonline-title-instrument": 1000,
    "landonline-title-instrument-title": 1000,
    "landonline-appellation": 5000,
    "landonline-legal-description": 5000,
    "landonline-legal-description-parcel": 5000,
    "aims-address-component-deprecated": 3000,
    "aims-address-reference-deprecated": 3000,
    "aims-address-deprecated": 3000,
    "aims-addressable-object-deprecated": 3000,
    "aims-addressable-object-external-deprecated": 3000,
}

MAX_BATCH_WRITE_RETRIES = 6


def parse_cli_args() -> dict:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--source_table")
    parser.add_argument("--source_dir")
    parser.add_argument("--extract_date")
    parser.add_argument("--catalog")
    parser.add_argument("--bronze_schema")
    parser.add_argument("--ops_schema")
    parser.add_argument("--chunk_size")
    parser.add_argument("--overwrite")
    args, _ = parser.parse_known_args()
    return {k: v for k, v in vars(args).items() if v is not None}


def get_param(name: str, cli_args: dict, default: str | None = None, required: bool = False) -> str:
    if name in cli_args and str(cli_args[name]).strip() != "":
        val = cli_args[name]
    else:
        try:
            dbutils.widgets.get(name)  # type: ignore[name-defined]
        except Exception:
            try:
                dbutils.widgets.text(name, default or "")  # type: ignore[name-defined]
            except Exception:
                pass
        try:
            val = dbutils.widgets.get(name)  # type: ignore[name-defined]
        except Exception:
            val = default

    if required and (val is None or str(val).strip() == ""):
        raise ValueError(f"Missing required parameter: {name}")
    return str(val) if val is not None else ""


def q(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def chunks(records: Iterable[dict], size: int) -> Iterator[List[dict]]:
    batch = []
    for record in records:
        batch.append(record)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def normalize_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        return v if v != "" else None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def normalize_record(record: dict, field_names: list[str]) -> dict:
    return {name.lower(): normalize_value(record.get(name)) for name in field_names}


def make_string_schema(field_names: list[str]) -> StructType:
    return StructType([StructField(n.lower(), StringType(), True) for n in field_names])


def write_batch_with_retry(df, target_table: str, write_mode: str) -> None:
    last_error = None
    for attempt in range(1, MAX_BATCH_WRITE_RETRIES + 1):
        try:
            (
                df.write.format("delta")
                .mode(write_mode)
                .option("overwriteSchema", "true" if write_mode == "overwrite" else "false")
                .saveAsTable(target_table)
            )
            return
        except Exception as e:  # noqa: BLE001
            last_error = e
            print(f"write failed attempt {attempt}/{MAX_BATCH_WRITE_RETRIES}: {e}")
            if attempt < MAX_BATCH_WRITE_RETRIES:
                sleep_s = attempt * 15
                print(f"sleep {sleep_s}s then retry")
                time.sleep(sleep_s)
    raise last_error


def main() -> None:
    cli_args = parse_cli_args()

    source_table = get_param("source_table", cli_args, required=True)
    source_dir = get_param("source_dir", cli_args, "/Volumes/main/default/linz_data/lds-world-10layers-unzipped")
    extract_date = get_param("extract_date", cli_args, "2026-04-06")
    catalog = get_param("catalog", cli_args, "main")
    bronze_schema = get_param("bronze_schema", cli_args, "bronze")
    ops_schema = get_param("ops_schema", cli_args, "ops")
    overwrite = get_param("overwrite", cli_args, "true").lower() == "true"
    chunk_size_raw = get_param("chunk_size", cli_args, "")

    if source_table not in TABLE_TO_TARGET:
        raise ValueError(f"Unknown source_table '{source_table}'. Allowed: {sorted(TABLE_TO_TARGET.keys())}")

    chunk_size = int(chunk_size_raw) if chunk_size_raw.strip() else DEFAULT_CHUNK_BY_TABLE[source_table]

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{ops_schema}")

    pattern = os.path.join(source_dir, source_table, f"{source_table}*.dbf")
    dbf_paths = sorted(glob.glob(pattern))
    if not dbf_paths:
        raise FileNotFoundError(f"No DBF files matched: {pattern}")

    target_table = q(catalog, bronze_schema, TABLE_TO_TARGET[source_table])
    run_id = str(uuid.uuid4())

    print(f"RUN_ID={run_id}")
    print(f"SOURCE_TABLE={source_table}")
    print(f"TARGET_TABLE={target_table}")
    print(f"DBF_FILES={len(dbf_paths)}")
    for p in dbf_paths:
        print(f"  - {p}")
    print(f"CHUNK_SIZE={chunk_size}")

    total_rows = 0
    first = True
    started = datetime.now(UTC)
    global_batch_num = 0

    for dbf_path in dbf_paths:
        file_batch_id = str(uuid.uuid4())
        dbf = DBF(dbf_path, load=False, char_decode_errors="ignore")
        field_names = [f.name for f in dbf.fields]
        schema = make_string_schema(field_names)

        for batch in chunks((normalize_record(r, field_names) for r in dbf), chunk_size):
            if not batch:
                continue

            global_batch_num += 1
            df = spark.createDataFrame(batch, schema=schema)
            df = (
                df.withColumn("source_file", F.lit(dbf_path))
                .withColumn("source_table", F.lit(source_table))
                .withColumn("source_extract_date", F.to_date(F.lit(extract_date)))
                .withColumn("ingested_at", F.current_timestamp())
                .withColumn("ingest_batch_id", F.lit(file_batch_id))
                .withColumn("run_id", F.lit(run_id))
            )

            mode = "overwrite" if (overwrite and first) else "append"
            write_batch_with_retry(df, target_table, mode)

            total_rows += len(batch)
            first = False
            if global_batch_num % 20 == 0 or len(batch) < chunk_size:
                print(f"[{source_table}] batch {global_batch_num} total={total_rows:,}")

    finished = datetime.now(UTC)
    print(f"DONE {source_table}: {total_rows:,} rows in {(finished - started).total_seconds():.1f}s")

    # lightweight audit row
    audit_table = q(catalog, ops_schema, "linz_ingest_progress")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
          run_id STRING,
          source_table STRING,
          target_table STRING,
          row_count BIGINT,
          status STRING,
          started_at TIMESTAMP,
          finished_at TIMESTAMP,
          message STRING
        ) USING DELTA
        """
    )

    audit_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("row_count", StringType(), False),
        StructField("status", StringType(), False),
        StructField("started_at", StringType(), False),
        StructField("finished_at", StringType(), False),
        StructField("message", StringType(), True),
    ])

    row = [{
        "run_id": run_id,
        "source_table": source_table,
        "target_table": target_table,
        "row_count": str(total_rows),
        "status": "SUCCESS",
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "message": "",
    }]

    (
        spark.createDataFrame(row, schema=audit_schema)
        .withColumn("row_count", F.col("row_count").cast("bigint"))
        .withColumn("started_at", F.to_timestamp("started_at"))
        .withColumn("finished_at", F.to_timestamp("finished_at"))
        .write.format("delta").mode("append").saveAsTable(audit_table)
    )


if __name__ == "__main__":
    main()
