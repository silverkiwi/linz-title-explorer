#!/usr/bin/env python3
"""
Reliable LINZ DBF -> Delta bronze loader (Databricks).

Designed for Databricks job/notebook execution with Spark available.
- Loads all 10 DBF tables safely, one table at a time
- Uses explicit STRING schema to avoid CANNOT_DETERMINE_TYPE inference issues
- Retries batch writes and whole-table loads
- Writes audit/progress records to main.ops tables

Before running:
  %pip install dbfread
  (then restart Python)

Example run (Databricks notebook cell):
  %run ./scripts/linz_ingest_all_safe.py

Or as a Python job/script:
  python scripts/linz_ingest_all_safe.py
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Iterable, Iterator, List

from dbfread import DBF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ==============================
# CONFIG (edit these)
# ==============================
CATALOG = "main"
BRONZE_SCHEMA = "bronze"
OPS_SCHEMA = "ops"
SOURCE_DIR = "/Volumes/main/default/linz_data/lds-world-10layers-unzipped"
EXTRACT_DATE = "2026-04-06"

# Default chunk size; overridden per table below where needed.
DEFAULT_CHUNK_SIZE = 10_000

# Very large tables can be safer at smaller chunk sizes.
CHUNK_SIZE_BY_TABLE = {
    "landonline-title": 10_000,
    "landonline-title-estate": 5_000,
    "landonline-title-instrument": 2_000,
    "landonline-title-instrument-title": 2_000,
}

MAX_BATCH_WRITE_RETRIES = 3
MAX_TABLE_RETRIES = 2

# If True, continue loading other tables if one table fails.
CONTINUE_ON_ERROR = False

# If True, overwrite each table on every run (recommended for reliability/idempotency).
OVERWRITE_EACH_TABLE = True

TABLES_IN_ORDER = [
    "landonline-transaction-type",
    "landonline-title-document-reference",
    "landonline-title",
    "landonline-title-hierarchy",
    "landonline-encumbrance",
    "landonline-encumbrance-share",
    "landonline-title-encumbrance",
    "landonline-title-estate",
    "landonline-title-instrument",
    "landonline-title-instrument-title",
]

TABLE_TO_TARGET = {
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


@dataclass
class TableResult:
    source_table: str
    target_table: str
    row_count: int
    status: str
    started_at: datetime
    finished_at: datetime
    message: str | None = None


def q(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}" if CATALOG else f"{schema}.{table}"


def table_dbf_path(source_table: str) -> str:
    return os.path.join(SOURCE_DIR, source_table, f"{source_table}.dbf")


def ensure_schemas(spark: SparkSession) -> None:
    if CATALOG:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(BRONZE_SCHEMA, '')[:-1]}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(OPS_SCHEMA, '')[:-1]}")


def ensure_ops_tables(spark: SparkSession) -> None:
    runs_table = q(OPS_SCHEMA, "linz_ingest_runs")
    progress_table = q(OPS_SCHEMA, "linz_ingest_progress")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {runs_table} (
          run_id STRING,
          extract_date DATE,
          source_dir STRING,
          started_at TIMESTAMP,
          finished_at TIMESTAMP,
          status STRING,
          message STRING
        ) USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {progress_table} (
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


def chunks(records: Iterable[dict], size: int) -> Iterator[List[dict]]:
    batch: List[dict] = []
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
    out = {}
    for name in field_names:
        out[name.lower()] = normalize_value(record.get(name))
    return out


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
            print(f"  write retry {attempt}/{MAX_BATCH_WRITE_RETRIES} failed for {target_table}: {e}")
            if attempt < MAX_BATCH_WRITE_RETRIES:
                backoff = attempt * 10
                print(f"  sleeping {backoff}s before retry...")
                time.sleep(backoff)
    raise last_error


def load_one_table(spark: SparkSession, source_table: str, run_id: str) -> TableResult:
    target_table = q(BRONZE_SCHEMA, TABLE_TO_TARGET[source_table])
    dbf_path = table_dbf_path(source_table)
    chunk_size = CHUNK_SIZE_BY_TABLE.get(source_table, DEFAULT_CHUNK_SIZE)

    if not os.path.exists(dbf_path):
        raise FileNotFoundError(f"Missing DBF file: {dbf_path}")

    started = datetime.now(UTC)

    for table_attempt in range(1, MAX_TABLE_RETRIES + 1):
        try:
            print(f"[{source_table}] start attempt {table_attempt}/{MAX_TABLE_RETRIES} | chunk={chunk_size} | target={target_table}")

            batch_id = str(uuid.uuid4())
            dbf = DBF(dbf_path, load=False, char_decode_errors="ignore")
            field_names = [f.name for f in dbf.fields]
            schema = make_string_schema(field_names)

            total_rows = 0
            first_batch = True

            for batch_num, batch in enumerate(
                chunks((normalize_record(r, field_names) for r in dbf), chunk_size),
                start=1,
            ):
                if not batch:
                    continue

                df = spark.createDataFrame(batch, schema=schema)
                df = (
                    df.withColumn("source_file", F.lit(dbf_path))
                    .withColumn("source_table", F.lit(source_table))
                    .withColumn("source_extract_date", F.to_date(F.lit(EXTRACT_DATE)))
                    .withColumn("ingested_at", F.current_timestamp())
                    .withColumn("ingest_batch_id", F.lit(batch_id))
                    .withColumn("run_id", F.lit(run_id))
                )

                mode = "overwrite" if (OVERWRITE_EACH_TABLE and first_batch) else "append"
                write_batch_with_retry(df, target_table, mode)

                total_rows += len(batch)
                first_batch = False

                if batch_num % 20 == 0 or len(batch) < chunk_size:
                    print(f"[{source_table}] batch {batch_num} | total {total_rows:,}")

            finished = datetime.now(UTC)
            return TableResult(
                source_table=source_table,
                target_table=target_table,
                row_count=total_rows,
                status="SUCCESS",
                started_at=started,
                finished_at=finished,
                message=None,
            )

        except Exception as e:  # noqa: BLE001
            print(f"[{source_table}] attempt {table_attempt} failed: {e}")
            if table_attempt < MAX_TABLE_RETRIES:
                sleep_for = table_attempt * 20
                print(f"[{source_table}] retrying full table in {sleep_for}s...")
                time.sleep(sleep_for)
            else:
                finished = datetime.now(UTC)
                return TableResult(
                    source_table=source_table,
                    target_table=target_table,
                    row_count=0,
                    status="FAILED",
                    started_at=started,
                    finished_at=finished,
                    message=str(e),
                )


def write_progress(spark: SparkSession, run_id: str, result: TableResult) -> None:
    progress_table = q(OPS_SCHEMA, "linz_ingest_progress")
    schema = StructType([
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
        "run_id": str(run_id),
        "source_table": str(result.source_table),
        "target_table": str(result.target_table),
        "row_count": str(result.row_count),
        "status": str(result.status),
        "started_at": result.started_at.isoformat(),
        "finished_at": result.finished_at.isoformat(),
        "message": result.message if result.message is not None else "",
    }]
    (
        spark.createDataFrame(row, schema=schema)
        .withColumn("row_count", F.col("row_count").cast("bigint"))
        .withColumn("started_at", F.to_timestamp("started_at"))
        .withColumn("finished_at", F.to_timestamp("finished_at"))
        .write.format("delta").mode("append").saveAsTable(progress_table)
    )


def write_run(spark: SparkSession, run_id: str, status: str, started_at: datetime, message: str | None = None) -> None:
    runs_table = q(OPS_SCHEMA, "linz_ingest_runs")
    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("extract_date", StringType(), False),
        StructField("source_dir", StringType(), False),
        StructField("started_at", StringType(), False),
        StructField("finished_at", StringType(), False),
        StructField("status", StringType(), False),
        StructField("message", StringType(), True),
    ])
    row = [{
        "run_id": str(run_id),
        "extract_date": str(EXTRACT_DATE),
        "source_dir": str(SOURCE_DIR),
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "status": str(status),
        "message": message if message is not None else "",
    }]
    (
        spark.createDataFrame(row, schema=schema)
        .withColumn("extract_date", F.to_date("extract_date"))
        .withColumn("started_at", F.to_timestamp("started_at"))
        .withColumn("finished_at", F.to_timestamp("finished_at"))
        .write.format("delta").mode("append").saveAsTable(runs_table)
    )


def preflight() -> None:
    missing = []
    for table in TABLES_IN_ORDER:
        p = table_dbf_path(table)
        if not os.path.exists(p):
            missing.append(p)
    if missing:
        raise FileNotFoundError("Missing DBF files:\n" + "\n".join(missing))



def main() -> None:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    ensure_schemas(spark)
    ensure_ops_tables(spark)
    preflight()

    run_id = str(uuid.uuid4())
    run_started = datetime.now(UTC)

    print(f"RUN_ID={run_id}")
    print(f"SOURCE_DIR={SOURCE_DIR}")
    print(f"EXTRACT_DATE={EXTRACT_DATE}")

    failures = []

    for source_table in TABLES_IN_ORDER:
        result = load_one_table(spark, source_table, run_id)
        write_progress(spark, run_id, result)

        if result.status == "SUCCESS":
            print(f"✓ {source_table} -> {result.target_table} ({result.row_count:,} rows)")
        else:
            print(f"✗ {source_table} FAILED: {result.message}")
            failures.append(result)
            if not CONTINUE_ON_ERROR:
                break

    if failures:
        write_run(
            spark,
            run_id,
            status="FAILED",
            started_at=run_started,
            message=f"{len(failures)} table(s) failed. First: {failures[0].source_table}",
        )
        raise RuntimeError(f"Ingest failed for {len(failures)} table(s).")
    else:
        write_run(spark, run_id, status="SUCCESS", started_at=run_started, message="All tables loaded")
        print("All tables loaded successfully.")


if __name__ == "__main__":
    main()
