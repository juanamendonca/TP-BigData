#!/usr/bin/env python3
"""
Bronze -> Silver pipeline (batch) for second partial point 3.

Implements minimum Silver requirements:
- Conformance and cleaning for usage events.
- Enrichment join with one master table (customers_orgs).
- 3 active data-quality rules.
- Quarantine dataset with samples of broken records.
- At least 3 features (daily_cost_usd, requests, genai_tokens_total, carbon_kg_total).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


@dataclass
class SilverStats:
    records_bronze_input: int
    records_after_event_dedupe: int
    records_quarantine: int
    records_silver_events: int
    records_silver_features: int


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
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Silver layer from Bronze usage events + customers_orgs"
    )
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
    parser.add_argument(
        "--sample-quarantine",
        type=int,
        default=100,
        help="Number of quarantine sample records to store for evidence.",
    )
    return parser.parse_args(argv)


def load_bronze_inputs(spark: SparkSession, bronze_root: Path) -> tuple[DataFrame, DataFrame]:
    usage_path = bronze_root / "streaming" / "usage_events"
    customers_path = bronze_root / "batch" / "customers_orgs"

    if not has_parquet_data(usage_path):
        raise FileNotFoundError(
            "No Bronze usage parquet data found. Expected path: "
            f"{usage_path}."
        )

    usage = spark.read.parquet(str(usage_path))
    customers = spark.read.parquet(str(customers_path)).select(
        "org_id",
        "org_name",
        "industry",
        "plan_tier",
        "lifecycle_stage",
        "hq_region",
        "is_enterprise",
    )
    return usage, customers


def prepare_events(usage: DataFrame) -> DataFrame:
    prepared = (
        usage.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("ingest_ts", F.to_timestamp("ingest_ts"))
        .withColumn("value_num", F.col("value").cast("double"))
        .withColumn("cost_usd_increment", F.col("cost_usd_increment").cast("double"))
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens").cast("long"), F.lit(0)))
        .withColumn("carbon_kg", F.coalesce(F.col("carbon_kg").cast("double"), F.lit(0.0)))
        .withColumn("metric", F.lower(F.trim(F.col("metric"))))
        .withColumn("unit", F.trim(F.col("unit")))
    )
    return prepared


def apply_quality_rules(events: DataFrame) -> DataFrame:
    w = Window.partitionBy("event_id")

    with_flags = (
        events.withColumn("event_id_count", F.count(F.lit(1)).over(w))
        .withColumn("dq_event_id_not_null", F.col("event_id").isNotNull())
        .withColumn("dq_event_id_unique", F.col("event_id_count") == F.lit(1))
        .withColumn(
            "dq_cost_min_threshold",
            F.coalesce(F.col("cost_usd_increment"), F.lit(0.0)) >= F.lit(-0.01),
        )
        .withColumn(
            "dq_unit_when_value",
            (~F.col("value_num").isNotNull()) | (F.length(F.col("unit")) > 0),
        )
        .withColumn("anomaly_cost_flag", ~F.col("dq_cost_min_threshold"))
    )

    with_reasons = with_flags.withColumn(
        "dq_violations",
        F.expr(
            """
            filter(
              array(
                case when not dq_event_id_not_null then 'event_id_null' end,
                case when not dq_event_id_unique then 'event_id_duplicate' end,
                case when not dq_cost_min_threshold then 'cost_below_-0.01' end,
                case when not dq_unit_when_value then 'unit_missing_when_value_present' end
              ),
              x -> x is not null
            )
            """
        ),
    )

    return with_reasons


def split_silver_and_quarantine(flagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    # Hard failures are quarantined. Cost anomalies remain in silver with anomaly_cost_flag.
    quarantine = flagged.filter(
        (~F.col("dq_event_id_not_null"))
        | (~F.col("dq_event_id_unique"))
        | (~F.col("dq_unit_when_value"))
    )

    silver_events = flagged.filter(
        F.col("dq_event_id_not_null")
        & F.col("dq_event_id_unique")
        & F.col("dq_unit_when_value")
    )

    return silver_events, quarantine


def enrich_with_master(silver_events: DataFrame, customers: DataFrame) -> DataFrame:
    return silver_events.join(customers, on="org_id", how="left")


def build_daily_features(enriched_events: DataFrame) -> DataFrame:
    return (
        enriched_events.groupBy("event_date", "org_id", "service")
        .agg(
            F.sum(F.coalesce(F.col("cost_usd_increment"), F.lit(0.0))).alias("daily_cost_usd"),
            F.sum(
                F.when(F.col("metric") == F.lit("requests"), F.coalesce(F.col("value_num"), F.lit(1.0))).otherwise(
                    F.lit(0.0)
                )
            ).alias("requests"),
            F.sum(F.coalesce(F.col("genai_tokens"), F.lit(0))).alias("genai_tokens_total"),
            F.sum(F.coalesce(F.col("carbon_kg"), F.lit(0.0))).alias("carbon_kg_total"),
            F.count(F.lit(1)).alias("events_count"),
            F.sum(F.when(F.col("anomaly_cost_flag"), F.lit(1)).otherwise(F.lit(0))).alias(
                "anomaly_events_count"
            ),
        )
        .withColumn("requests", F.col("requests").cast("long"))
        .withColumn("genai_tokens_total", F.col("genai_tokens_total").cast("long"))
    )


def write_outputs(
    silver_root: Path,
    enriched_events: DataFrame,
    daily_features: DataFrame,
    quarantine: DataFrame,
    sample_quarantine: int,
) -> None:
    events_path = silver_root / "events_enriched"
    features_path = silver_root / "features_org_daily"
    quarantine_path = silver_root / "quarantine" / "events_quality_issues"
    quarantine_samples_path = silver_root / "quarantine" / "samples"

    (
        enriched_events.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(events_path))
    )
    (
        daily_features.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(features_path))
    )
    (
        quarantine.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(quarantine_path))
    )

    sample = quarantine.select(
        "event_id",
        "event_ts",
        "org_id",
        "service",
        "metric",
        "value",
        "unit",
        "cost_usd_increment",
        "dq_violations",
        "source_file",
    ).limit(sample_quarantine)
    sample.write.mode("overwrite").json(str(quarantine_samples_path))


def print_verify_counts(spark: SparkSession, silver_root: Path) -> None:
    verify_paths = [
        ("silver/events_enriched", silver_root / "events_enriched"),
        ("silver/features_org_daily", silver_root / "features_org_daily"),
        ("silver/quarantine/events_quality_issues", silver_root / "quarantine" / "events_quality_issues"),
    ]

    for label, path in verify_paths:
        if has_parquet_data(path):
            total_rows = spark.read.parquet(str(path)).count()
            print(
                f"[VERIFY] {label} total_rows={total_rows} "
                "(compare this value across reruns for idempotency)"
            )
        else:
            print(f"[WARN] Verification skipped: no parquet data found at {path}")


def write_manifest(
    silver_root: Path,
    stats: SilverStats,
    started_at: str,
    finished_at: str,
) -> None:
    control_dir = silver_root / "_control"
    ensure_dir(control_dir)

    manifest = {
        "pipeline": "bronze_to_silver",
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "stats": asdict(stats),
        "quality_rules": [
            "event_id not null",
            "event_id unique",
            "cost_usd_increment >= -0.01 (anomaly flag)",
            "unit not null when value exists",
        ],
        "silver_outputs": [
            "datalake/silver/events_enriched",
            "datalake/silver/features_org_daily",
            "datalake/silver/quarantine/events_quality_issues",
            "datalake/silver/quarantine/samples",
        ],
    }

    with (control_dir / "manifest.json").open("w", encoding="utf-8") as w:
        json.dump(manifest, w, indent=2, ensure_ascii=False)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    ensure_dir(args.silver_root)

    started_at = utc_now_iso()
    spark = build_spark("bronze_to_silver")

    try:
        usage, customers = load_bronze_inputs(spark, args.bronze_root)

        records_bronze_input = usage.count()
        prepared = prepare_events(usage)
        records_after_event_dedupe = prepared.dropDuplicates(["event_id"]).count()

        flagged = apply_quality_rules(prepared)
        silver_events, quarantine = split_silver_and_quarantine(flagged)

        records_flagged_total = flagged.count()
        records_hard_fail = quarantine.count()
        records_pre_enrichment = silver_events.count()

        records_without_customer_match = (
            silver_events.select("org_id").dropDuplicates(["org_id"])
            .join(customers.select("org_id").dropDuplicates(["org_id"]), on="org_id", how="left_anti")
            .count()
        )

        enriched = enrich_with_master(silver_events, customers)
        features = build_daily_features(enriched)

        records_quarantine = records_hard_fail
        records_silver_events = enriched.count()
        records_silver_features = features.count()

        write_outputs(
            silver_root=args.silver_root,
            enriched_events=enriched,
            daily_features=features,
            quarantine=quarantine,
            sample_quarantine=args.sample_quarantine,
        )

        print_verify_counts(spark, args.silver_root)

        stats = SilverStats(
            records_bronze_input=records_bronze_input,
            records_after_event_dedupe=records_after_event_dedupe,
            records_quarantine=records_quarantine,
            records_silver_events=records_silver_events,
            records_silver_features=records_silver_features,
        )

        finished_at = utc_now_iso()
        write_manifest(args.silver_root, stats, started_at, finished_at)

        print(f"[INFO] Bronze records: {records_bronze_input}")
        print(f"[INFO] After dedupe: {records_after_event_dedupe}")
        print(f"[INFO] Flagged total (before split): {records_flagged_total}")
        print(f"[INFO] Hard-fail quality records: {records_hard_fail}")
        print(f"[INFO] Silver events before enrichment: {records_pre_enrichment}")
        print(f"[INFO] Distinct org_id without customers match: {records_without_customer_match}")
        print(f"[INFO] Quarantine records: {records_quarantine}")
        print(f"[INFO] Silver events: {records_silver_events}")
        print(f"[INFO] Silver features: {records_silver_features}")
        print(f"[OK] Silver pipeline completed. Manifest: {args.silver_root / '_control' / 'manifest.json'}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
