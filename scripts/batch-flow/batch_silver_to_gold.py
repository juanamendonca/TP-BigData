#!/usr/bin/env python3
"""
Batch Silver -> Gold pipeline.
Builds batch Gold marts:
- revenue_by_org_month (monthly grain)
- tickets_by_org_date (daily grain + severity_breakdown map collection)
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
    parser = argparse.ArgumentParser(description="Batch Silver to Gold pipeline.")
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=Path("datalake") / "silver",
        help="Silver root path.",
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=Path("datalake") / "gold",
        help="Gold root path.",
    )
    return parser.parse_args(argv)


def build_revenue_mart(spark: SparkSession, silver_root: Path, gold_root: Path) -> None:
    silver_billing_path = silver_root / "billing_monthly_normalized"
    if not has_parquet_data(silver_billing_path):
        print(f"[WARN] No Silver billing data found at {silver_billing_path}. Skipping revenue mart.")
        return

    print(f"[INFO] Building revenue mart from {silver_billing_path}...")
    df = spark.read.parquet(str(silver_billing_path))

    # Aggregating to monthly grain per org
    # net_revenue_usd = revenue_usd - credits_usd + taxes_usd
    revenue_mart = (
        df.groupBy("org_id", "month")
        .agg(
            F.sum("revenue_usd").alias("revenue_usd"),
            F.sum("credits_usd").alias("credits_usd"),
            F.sum("taxes_usd").alias("taxes_usd"),
            F.avg("fx_applied").alias("fx_applied")
        )
        .withColumn("net_revenue_usd", F.col("revenue_usd") - F.col("credits_usd") + F.col("taxes_usd"))
        # Round double values to 6 decimals for cleaner storage
        .withColumn("revenue_usd", F.round("revenue_usd", 6))
        .withColumn("credits_usd", F.round("credits_usd", 6))
        .withColumn("taxes_usd", F.round("taxes_usd", 6))
        .withColumn("net_revenue_usd", F.round("net_revenue_usd", 6))
        .withColumn("fx_applied", F.round("fx_applied", 6))
    )

    gold_out = gold_root / "revenue_by_org_month"
    revenue_mart.write.mode("overwrite").partitionBy("month").parquet(str(gold_out))
    print(f"[OK] Gold revenue mart written to {gold_out}. Count: {revenue_mart.count()}")


def build_tickets_mart(spark: SparkSession, silver_root: Path, gold_root: Path) -> None:
    silver_tickets_path = silver_root / "tickets_by_org_date"
    if not has_parquet_data(silver_tickets_path):
        print(f"[WARN] No Silver tickets data found at {silver_tickets_path}. Skipping tickets mart.")
        return

    print(f"[INFO] Building tickets mart from {silver_tickets_path}...")
    df = spark.read.parquet(str(silver_tickets_path))

    # Aggregate to daily grain per org, constructing the severity_breakdown map collection
    # CSAT avg and SLA breach rate must be daily weighted averages
    tickets_mart = (
        df.withColumn("weighted_csat", F.col("csat_avg") * F.col("ticket_count"))
        .withColumn("weighted_sla", F.col("sla_breach_rate") * F.col("ticket_count"))
        .groupBy("org_id", F.col("created_date").alias("event_date"))
        .agg(
            F.sum("ticket_count").alias("ticket_count"),
            F.coalesce(F.sum("weighted_sla") / F.sum("ticket_count"), F.lit(0.0)).alias("sla_breach_rate"),
            # Weighted average csat, ignoring nulls
            F.coalesce(
                F.sum(F.when(F.col("csat_avg").isNotNull(), F.col("weighted_csat"))) /
                F.sum(F.when(F.col("csat_avg").isNotNull(), F.col("ticket_count"))),
                F.lit(None).cast("double")
            ).alias("csat_avg"),
            # Create a map for severity breakdown: severity -> count
            F.create_map(
                F.lit("low"), F.coalesce(F.sum(F.when(F.col("severity") == "low", F.col("ticket_count"))), F.lit(0).cast("long")),
                F.lit("medium"), F.coalesce(F.sum(F.when(F.col("severity") == "medium", F.col("ticket_count"))), F.lit(0).cast("long")),
                F.lit("high"), F.coalesce(F.sum(F.when(F.col("severity") == "high", F.col("ticket_count"))), F.lit(0).cast("long")),
                F.lit("critical"), F.coalesce(F.sum(F.when(F.col("severity") == "critical", F.col("ticket_count"))), F.lit(0).cast("long"))
            ).alias("severity_breakdown")
        )
        .withColumn("month_bucket", F.date_format(F.col("event_date"), "yyyy-MM"))
        .withColumn("sla_breach_rate", F.round("sla_breach_rate", 6))
        .withColumn("csat_avg", F.round("csat_avg", 6))
    )

    gold_out = gold_root / "tickets_by_org_date"
    tickets_mart.write.mode("overwrite").partitionBy("event_date").parquet(str(gold_out))
    print(f"[OK] Gold tickets mart written to {gold_out}. Count: {tickets_mart.count()}")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    ensure_dir(args.gold_root)

    spark = build_spark("batch_silver_to_gold")
    try:
        build_revenue_mart(spark, args.silver_root, args.gold_root)
        build_tickets_mart(spark, args.silver_root, args.gold_root)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    sys.exit(main())
