#!/usr/bin/env python3
"""
Batch Bronze -> Silver pipeline.
Cleans and normalizes:
- billing_monthly: normalizes currency to USD, validates, writes to silver & quarantine.
- support_tickets: validates, aggregates daily features by org/severity, writes to silver & quarantine.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


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
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch Bronze to Silver pipeline for billing and tickets.")
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=Path("datalake") / "bronze",
        help="Bronze root path.",
    )
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=Path("datalake") / "silver",
        help="Silver root path.",
    )
    return parser.parse_args(argv)


def process_billing(spark: SparkSession, bronze_root: Path, silver_root: Path) -> None:
    bronze_billing_path = bronze_root / "batch" / "billing_monthly"
    if not has_parquet_data(bronze_billing_path):
        print(f"[WARN] No Bronze billing data found at {bronze_billing_path}. Skipping billing.")
        return

    print(f"[INFO] Processing billing from {bronze_billing_path}...")
    df = spark.read.parquet(str(bronze_billing_path))

    # Apply currency normalization to USD
    # subtotal, credits, taxes multiplied by exchange_rate_to_usd
    # Credits and taxes replaced with 0.0 if null
    normalized = (
        df.withColumn("credits_clean", F.coalesce(F.col("credits"), F.lit(0.0)))
        .withColumn("taxes_clean", F.coalesce(F.col("taxes"), F.lit(0.0)))
        .withColumn("exchange_rate", F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0)))
        .withColumn("revenue_usd", F.col("subtotal") * F.col("exchange_rate"))
        .withColumn("credits_usd", F.col("credits_clean") * F.col("exchange_rate"))
        .withColumn("taxes_usd", F.col("taxes_clean") * F.col("exchange_rate"))
        .withColumn("fx_applied", F.col("exchange_rate"))
        .drop("credits_clean", "taxes_clean", "exchange_rate")
    )

    # Validation rules
    # org_id not null, month not null, revenue_usd > 0
    valid_cond = (
        F.col("org_id").isNotNull()
        & F.col("month").isNotNull()
        & (F.col("revenue_usd") > 0.0)
    )

    valid_df = normalized.filter(valid_cond)
    quarantine_df = normalized.filter(~valid_cond)

    valid_out = silver_root / "billing_monthly_normalized"
    quarantine_out = silver_root / "quarantine" / "billing"

    # Write Silver
    valid_df.write.mode("overwrite").partitionBy("month").parquet(str(valid_out))
    print(f"[OK] Silver billing written to {valid_out}. Count: {valid_df.count()}")

    # Write Quarantine
    if quarantine_df.count() > 0:
        quarantine_df.write.mode("overwrite").parquet(str(quarantine_out))
        print(f"[WARN] Quarantined {quarantine_df.count()} billing records at {quarantine_out}")


def process_tickets(spark: SparkSession, bronze_root: Path, silver_root: Path) -> None:
    bronze_tickets_path = bronze_root / "batch" / "support_tickets"
    if not has_parquet_data(bronze_tickets_path):
        print(f"[WARN] No Bronze support tickets found at {bronze_tickets_path}. Skipping tickets.")
        return

    print(f"[INFO] Processing support tickets from {bronze_tickets_path}...")
    df = spark.read.parquet(str(bronze_tickets_path))

    # Validation rules
    # ticket_id not null, severity in valid values (low, medium, high, critical), csat null or in range [1, 5]
    valid_severities = ["low", "medium", "high", "critical"]
    valid_cond = (
        F.col("ticket_id").isNotNull()
        & F.col("severity").isin(valid_severities)
        & (F.col("csat").isNull() | ((F.col("csat") >= 1.0) & (F.col("csat") <= 5.0)))
    )

    valid_raw_df = df.filter(valid_cond)
    quarantine_df = df.filter(~valid_cond)

    quarantine_out = silver_root / "quarantine" / "tickets"
    # Write Quarantine
    if quarantine_df.count() > 0:
        quarantine_df.write.mode("overwrite").parquet(str(quarantine_out))
        print(f"[WARN] Quarantined {quarantine_df.count()} ticket records at {quarantine_out}")

    # Calculate daily aggregated features by org_id, created_date, severity
    # created_date is extracted from created_at timestamp
    aggregated = (
        valid_raw_df
        .withColumn("created_date", F.to_date("created_at"))
        .groupBy("org_id", "created_date", "severity")
        .agg(
            F.count(F.lit(1)).alias("ticket_count"),
            F.avg(F.col("sla_breached").cast("double")).alias("sla_breach_rate"),
            F.avg(F.col("csat")).alias("csat_avg")
        )
    )

    valid_out = silver_root / "tickets_by_org_date"
    # Write Silver aggregated tickets
    aggregated.write.mode("overwrite").partitionBy("created_date").parquet(str(valid_out))
    print(f"[OK] Silver tickets aggregated and written to {valid_out}. Count: {aggregated.count()}")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    ensure_dir(args.silver_root)

    spark = build_spark("batch_bronze_to_silver")
    try:
        process_billing(spark, args.bronze_root, args.silver_root)
        process_tickets(spark, args.bronze_root, args.silver_root)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    sys.exit(main())
