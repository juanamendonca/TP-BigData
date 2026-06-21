#!/usr/bin/env python3
"""
Batch ingestion pipeline: landing CSV -> bronze Parquet (PySpark).

Aligned with first + second partial requirements for batch Bronze:
- explicit schemas,
- Parquet output,
- sensible per-table partitioning,
- technical columns ingest_ts and source_file,
- basic non-null quality filters on critical keys,
- idempotent re-run by partition overwrite.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass
class TableConfig:
    csv_file: str
    schema: T.StructType
    dedupe_keys: List[str]
    partition_col: str
    required_not_null: List[str]


@dataclass
class TableStats:
    table: str
    source_path: str
    records_read: int
    records_after_quality: int
    records_after_dedupe: int
    records_written: int
    output_path: str
    dedupe_keys: List[str]
    partition_col: str
    required_not_null: List[str]


TABLES: Dict[str, TableConfig] = {
    "billing_monthly": TableConfig(
        csv_file="billing_monthly.csv",
        schema=T.StructType(
            [
                T.StructField("invoice_id", T.StringType(), True),
                T.StructField("org_id", T.StringType(), True),
                T.StructField("month", T.DateType(), True),
                T.StructField("subtotal", T.DoubleType(), True),
                T.StructField("credits", T.DoubleType(), True),
                T.StructField("taxes", T.DoubleType(), True),
                T.StructField("currency", T.StringType(), True),
                T.StructField("exchange_rate_to_usd", T.DoubleType(), True),
            ]
        ),
        dedupe_keys=["invoice_id"],
        partition_col="month",
        required_not_null=["invoice_id", "org_id", "month"],
    ),
    "customers_orgs": TableConfig(
        csv_file="customers_orgs.csv",
        schema=T.StructType(
            [
                T.StructField("org_id", T.StringType(), True),
                T.StructField("org_name", T.StringType(), True),
                T.StructField("industry", T.StringType(), True),
                T.StructField("hq_region", T.StringType(), True),
                T.StructField("plan_tier", T.StringType(), True),
                T.StructField("is_enterprise", T.BooleanType(), True),
                T.StructField("signup_date", T.DateType(), True),
                T.StructField("sales_rep", T.StringType(), True),
                T.StructField("lifecycle_stage", T.StringType(), True),
                T.StructField("marketing_source", T.StringType(), True),
                T.StructField("nps_score", T.DoubleType(), True),
            ]
        ),
        dedupe_keys=["org_id"],
        partition_col="load_date",
        required_not_null=["org_id"],
    ),
    "marketing_touches": TableConfig(
        csv_file="marketing_touches.csv",
        schema=T.StructType(
            [
                T.StructField("touch_id", T.StringType(), True),
                T.StructField("org_id", T.StringType(), True),
                T.StructField("campaign", T.StringType(), True),
                T.StructField("channel", T.StringType(), True),
                T.StructField("timestamp", T.TimestampType(), True),
                T.StructField("clicked", T.BooleanType(), True),
                T.StructField("converted", T.BooleanType(), True),
            ]
        ),
        dedupe_keys=["touch_id"],
        partition_col="touch_date",
        required_not_null=["touch_id", "org_id", "touch_date"],
    ),
    "nps_surveys": TableConfig(
        csv_file="nps_surveys.csv",
        schema=T.StructType(
            [
                T.StructField("org_id", T.StringType(), True),
                T.StructField("survey_date", T.DateType(), True),
                T.StructField("nps_score", T.DoubleType(), True),
                T.StructField("comment", T.StringType(), True),
            ]
        ),
        dedupe_keys=["org_id", "survey_date"],
        partition_col="survey_date",
        required_not_null=["org_id", "survey_date"],
    ),
    "resources": TableConfig(
        csv_file="resources.csv",
        schema=T.StructType(
            [
                T.StructField("resource_id", T.StringType(), True),
                T.StructField("org_id", T.StringType(), True),
                T.StructField("service", T.StringType(), True),
                T.StructField("region", T.StringType(), True),
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("state", T.StringType(), True),
                T.StructField("tags_json", T.StringType(), True),
            ]
        ),
        dedupe_keys=["resource_id"],
        partition_col="batch_date",
        required_not_null=["resource_id", "org_id"],
    ),
    "support_tickets": TableConfig(
        csv_file="support_tickets.csv",
        schema=T.StructType(
            [
                T.StructField("ticket_id", T.StringType(), True),
                T.StructField("org_id", T.StringType(), True),
                T.StructField("category", T.StringType(), True),
                T.StructField("severity", T.StringType(), True),
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("resolved_at", T.TimestampType(), True),
                T.StructField("csat", T.DoubleType(), True),
                T.StructField("sla_breached", T.BooleanType(), True),
            ]
        ),
        dedupe_keys=["ticket_id"],
        partition_col="created_date",
        required_not_null=["ticket_id", "org_id", "created_date"],
    ),
    "users": TableConfig(
        csv_file="users.csv",
        schema=T.StructType(
            [
                T.StructField("user_id", T.StringType(), True),
                T.StructField("org_id", T.StringType(), True),
                T.StructField("email", T.StringType(), True),
                T.StructField("role", T.StringType(), True),
                T.StructField("active", T.BooleanType(), True),
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("last_login", T.TimestampType(), True),
            ]
        ),
        dedupe_keys=["user_id"],
        partition_col="load_date",
        required_not_null=["user_id", "org_id"],
    ),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def has_parquet_data(path: Path) -> bool:
    if not path.exists():
        return False
    return any(path.rglob("*.parquet"))


def build_spark(app_name: str) -> SparkSession:
    warehouse_dir = (Path.cwd() / "datalake" / "_spark_warehouse").resolve()
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def load_master_csv(
    spark: SparkSession,
    table_name: str,
    cfg: TableConfig,
    landing_root: Path,
    batch_date: str,
) -> DataFrame:
    source_path = landing_root / cfg.csv_file
    if not source_path.exists():
        raise FileNotFoundError(f"Missing landing file for {table_name}: {source_path}")

    df = (
        spark.read.option("header", True)
        .schema(cfg.schema)
        .csv(str(source_path))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("batch_date", F.lit(batch_date).cast("date"))
        .withColumn("load_date", F.col("batch_date"))
    )

    if "created_at" in df.columns:
        df = df.withColumn("created_date", F.to_date("created_at"))
    if "timestamp" in df.columns:
        df = df.withColumn("touch_date", F.to_date("timestamp"))

    return df


def process_table(
    spark: SparkSession,
    table_name: str,
    cfg: TableConfig,
    landing_root: Path,
    bronze_root: Path,
    batch_date: str,
) -> TableStats:
    df_raw = load_master_csv(spark, table_name, cfg, landing_root, batch_date)
    records_read = df_raw.count()

    df_quality = df_raw
    for col_name in cfg.required_not_null:
        df_quality = df_quality.filter(F.col(col_name).isNotNull())
    records_after_quality = df_quality.count()

    df_deduped = df_quality.dropDuplicates(cfg.dedupe_keys)
    records_after_dedupe = df_deduped.count()

    output_path = bronze_root / "batch" / table_name
    (
        df_deduped.write.mode("overwrite")
        .partitionBy(cfg.partition_col)
        .parquet(str(output_path))
    )

    records_written = records_after_dedupe

    return TableStats(
        table=table_name,
        source_path=str(landing_root / cfg.csv_file),
        records_read=records_read,
        records_after_quality=records_after_quality,
        records_after_dedupe=records_after_dedupe,
        records_written=records_written,
        output_path=str(output_path),
        dedupe_keys=cfg.dedupe_keys,
        partition_col=cfg.partition_col,
        required_not_null=cfg.required_not_null,
    )


def write_manifest(
    bronze_root: Path,
    batch_date: str,
    started_at: str,
    finished_at: str,
    stats: List[TableStats],
) -> Path:
    control_dir = bronze_root / "_control" / f"batch_date={batch_date}"
    ensure_dir(control_dir)

    totals = {
        "tables_processed": len(stats),
        "records_read": sum(s.records_read for s in stats),
        "records_after_quality": sum(s.records_after_quality for s in stats),
        "records_after_dedupe": sum(s.records_after_dedupe for s in stats),
        "records_written": sum(s.records_written for s in stats),
    }

    manifest = {
        "pipeline": "batch_landing_to_bronze_pyspark",
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "batch_date": batch_date,
        "totals": totals,
        "tables": [asdict(s) for s in stats],
    }

    manifest_path = control_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as w:
        json.dump(manifest, w, indent=2, ensure_ascii=False)

    return manifest_path


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch load landing CSV masters into Bronze Parquet with PySpark."
    )
    parser.add_argument(
        "--landing-root",
        type=Path,
        default=Path("datalake") / "landing",
        help="Input landing directory.",
    )
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=Path("datalake") / "bronze",
        help="Output bronze directory.",
    )
    parser.add_argument(
        "--batch-date",
        type=str,
        default=datetime.now(timezone.utc).date().isoformat(),
        help="Partition date in YYYY-MM-DD. Default: UTC today.",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default=",".join(TABLES.keys()),
        help="Comma-separated table names to process. Defaults to all masters.",
    )
    return parser.parse_args(argv)


def validate_batch_date(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid --batch-date '{value}'. Use YYYY-MM-DD.") from exc
    return value


def parse_selected_tables(csv_text: str) -> List[str]:
    names = [item.strip() for item in csv_text.split(",") if item.strip()]
    invalid = [name for name in names if name not in TABLES]
    if invalid:
        raise ValueError(f"Unknown table(s): {', '.join(invalid)}")
    if len(names) < 3:
        raise ValueError("Second partial requires at least 3 masters in batch Bronze.")
    return names


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    try:
        batch_date = validate_batch_date(args.batch_date)
        selected_tables = parse_selected_tables(args.tables)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    landing_root: Path = args.landing_root
    bronze_root: Path = args.bronze_root

    if not landing_root.exists() or not landing_root.is_dir():
        print(f"[ERROR] Landing root does not exist: {landing_root}", file=sys.stderr)
        return 2

    ensure_dir(bronze_root)

    started_at = utc_now_iso()
    stats: List[TableStats] = []
    spark = build_spark("batch_landing_to_bronze")

    try:
        print(f"[INFO] Tables selected: {', '.join(selected_tables)}")
        for table_name in selected_tables:
            table_stats = process_table(
                spark=spark,
                table_name=table_name,
                cfg=TABLES[table_name],
                landing_root=landing_root,
                bronze_root=bronze_root,
                batch_date=batch_date,
            )
            stats.append(table_stats)
            print(
                f"[OK] {table_name}: read={table_stats.records_read} "
                f"after_quality={table_stats.records_after_quality} "
                f"after_dedupe={table_stats.records_after_dedupe} "
                f"written={table_stats.records_written}"
            )

        for verify_table in selected_tables:
            verify_path = bronze_root / "batch" / verify_table
            if has_parquet_data(verify_path):
                verify_count = spark.read.parquet(str(verify_path)).count()
                print(
                    f"[VERIFY] bronze/{verify_table} total_rows={verify_count} "
                    "(use this value to compare reruns for idempotency)"
                )
            else:
                print(
                    f"[WARN] Verification skipped: no parquet data found at {verify_path}"
                )
    finally:
        spark.stop()

    finished_at = utc_now_iso()
    manifest_path = write_manifest(
        bronze_root=bronze_root,
        batch_date=batch_date,
        started_at=started_at,
        finished_at=finished_at,
        stats=stats,
    )

    print(f"[INFO] Manifest: {manifest_path}")
    print(
        "[INFO] Totals: "
        f"read={sum(s.records_read for s in stats)} "
        f"after_quality={sum(s.records_after_quality for s in stats)} "
        f"after_dedupe={sum(s.records_after_dedupe for s in stats)} "
        f"written={sum(s.records_written for s in stats)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
