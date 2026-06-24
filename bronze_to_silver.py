#!/usr/bin/env python3
"""
Bronze -> Silver pipeline (Structured Streaming) for second partial point 3.

Implements minimum Silver requirements:
- Conformance and cleaning for usage events.
- Enrichment join with one master table (customers_orgs).
- 3 active data-quality rules.
- Quarantine dataset with samples of broken records.
- At least 3 features (daily_cost_usd, requests, genai_tokens_total, carbon_kg_total).
- Reads Bronze usage events with readStream and writes Silver outputs with writeStream.
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


BRONZE_USAGE_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("org_id", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("resource_id", T.StringType(), True),
        T.StructField("metric", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("unit", T.StringType(), True),
        T.StructField("cost_usd_increment", T.DoubleType(), True),
        T.StructField("schema_version", T.IntegerType(), True),
        T.StructField("genai_tokens", T.LongType(), True),
        T.StructField("carbon_kg", T.DoubleType(), True),
        T.StructField("_corrupt_record", T.StringType(), True),
        T.StructField("ingest_ts", T.TimestampType(), True),
        T.StructField("source_file", T.StringType(), True),
        T.StructField("event_ts", T.TimestampType(), True),
        T.StructField("batch_date", T.DateType(), True),
        T.StructField("value_num", T.DoubleType(), True),
        T.StructField("is_value_cast_error", T.BooleanType(), True),
        T.StructField("event_date", T.DateType(), True),
    ]
)


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
    spark.conf.set("spark.sql.shuffle.partitions", "4")
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
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=Path("datalake") / "checkpoints",
        help="Checkpoint root for streaming queries.",
    )
    parser.add_argument(
        "--watermark-delay",
        type=str,
        default="2 days",
        help="Watermark delay for event-time aggregations.",
    )
    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=50,
        help="Max Bronze Parquet files per micro-batch.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep streaming queries running continuously. Default drains backlog with availableNow.",
    )
    return parser.parse_args(argv)


def load_bronze_inputs(
    spark: SparkSession,
    bronze_root: Path,
    max_files_per_trigger: int,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    usage_path = bronze_root / "streaming" / "usage_events"
    customers_path = bronze_root / "batch" / "customers_orgs"
    users_path = bronze_root / "batch" / "users"
    resources_path = bronze_root / "batch" / "resources"

    if not has_parquet_data(usage_path):
        raise FileNotFoundError(
            "No Bronze usage parquet data found. Expected path: "
            f"{usage_path}."
        )

    usage = (
        spark.readStream.format("parquet")
        .schema(BRONZE_USAGE_SCHEMA)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(str(usage_path))
    )
    customers = spark.read.parquet(str(customers_path)).select(
        "org_id",
        "org_name",
        "industry",
        "plan_tier",
        "lifecycle_stage",
        "hq_region",
        "is_enterprise",
    )
    users = spark.read.parquet(str(users_path)).select(
        "org_id",
        "user_id",
        "active",
    )
    resources = spark.read.parquet(str(resources_path)).select(
        "resource_id",
        "state",
        "tags_json",
    )
    return usage, customers, users, resources


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
    with_flags = (
        events.withColumn("dq_event_id_not_null", F.col("event_id").isNotNull())
        # Bronze streaming already performs event_id dedupe with watermark. Silver keeps the
        # rule explicit and re-applies dropDuplicates before writing valid events.
        .withColumn("dq_event_id_unique", F.lit(True))
        .withColumn(
            "dq_cost_min_threshold",
            F.coalesce(F.col("cost_usd_increment"), F.lit(0.0)) >= F.lit(-0.01),
        )
        .withColumn(
            "dq_unit_when_value",
            (~F.col("value_num").isNotNull()) | (F.coalesce(F.length(F.col("unit")), F.lit(0)) > 0),
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

    silver_events = flagged.filter(F.col("dq_event_id_not_null") & F.col("dq_unit_when_value"))

    return silver_events, quarantine


def enrich_with_master(
    silver_events: DataFrame,
    customers: DataFrame,
    users: DataFrame,
    resources: DataFrame,
) -> DataFrame:
    # Pre-aggregate users per org to avoid cartesian product (since org has multiple users)
    users_agg = users.groupBy("org_id").agg(
        F.count("user_id").alias("total_org_users"),
        F.sum(F.when(F.col("active") == True, F.lit(1)).otherwise(F.lit(0))).alias("active_org_users")
    )
    
    # Rename resource columns to avoid collisions
    resources_clean = resources.select(
        F.col("resource_id"),
        F.col("state").alias("resource_state"),
        F.col("tags_json").alias("resource_tags_json")
    )
    
    # Join with orgs (customers), users, and resources
    enriched_orgs = silver_events.join(customers, on="org_id", how="left")
    enriched_users = enriched_orgs.join(users_agg, on="org_id", how="left")
    enriched_all = enriched_users.join(resources_clean, on="resource_id", how="left")
    
    return enriched_all


def build_daily_features(enriched_events: DataFrame, watermark_delay: str) -> DataFrame:
    spark = enriched_events.sparkSession
    
    # Path to Bronze usage events for static historical calculation
    usage_path = Path("datalake") / "bronze" / "streaming" / "usage_events"
    
    if has_parquet_data(usage_path):
        # Read static historical data
        static_bronze = spark.read.parquet(str(usage_path))
        
        # Calculate daily cost per org/service/date to get daily grain stats
        daily_costs_static = (
            static_bronze
            .withColumn("event_date", F.to_date("event_ts"))
            .groupBy("event_date", "org_id", "service")
            .agg(F.sum(F.coalesce(F.col("cost_usd_increment"), F.lit(0.0))).alias("daily_cost_usd"))
        )
        
        # Compute mean and stddev of daily_cost_usd per service
        service_stats = (
            daily_costs_static
            .groupBy("service")
            .agg(
                F.mean("daily_cost_usd").alias("service_mean_cost"),
                F.stddev("daily_cost_usd").alias("service_stddev_cost")
            )
        )
    else:
        # Fallback if no parquet files are written yet
        stats_schema = T.StructType([
            T.StructField("service", T.StringType(), True),
            T.StructField("service_mean_cost", T.DoubleType(), True),
            T.StructField("service_stddev_cost", T.DoubleType(), True),
        ])
        service_stats = spark.createDataFrame([], schema=stats_schema)

    df_features = (
        enriched_events
        .groupBy(F.window("event_ts", "1 day").alias("event_window"), "org_id", "service")
        .agg(
            F.sum(F.coalesce(F.col("cost_usd_increment"), F.lit(0.0))).alias("daily_cost_usd"),
            F.sum(
                F.when(F.col("metric") == F.lit("requests"), F.coalesce(F.col("value_num"), F.lit(1.0))).otherwise(
                    F.lit(0.0)
                )
            ).alias("requests"),
            F.sum(
                F.when(F.col("metric") == F.lit("cpu_hours"), F.coalesce(F.col("value_num"), F.lit(0.0))).otherwise(
                    F.lit(0.0)
                )
            ).alias("cpu_hours"),
            F.sum(
                F.when(F.col("metric") == F.lit("storage_gb_hours"), F.coalesce(F.col("value_num"), F.lit(0.0))).otherwise(
                    F.lit(0.0)
                )
            ).alias("storage_gb_hours"),
            F.sum(F.coalesce(F.col("genai_tokens"), F.lit(0))).alias("genai_tokens_total"),
            F.sum(F.coalesce(F.col("carbon_kg"), F.lit(0.0))).alias("carbon_kg_total"),
            F.count(F.lit(1)).alias("events_count"),
            F.sum(F.when(F.col("anomaly_cost_flag"), F.lit(1)).otherwise(F.lit(0))).alias(
                "anomaly_events_count"
            ),
        )
        .withColumn("event_date", F.to_date(F.col("event_window.start")))
        .drop("event_window")
        .withColumn("requests", F.col("requests").cast("long"))
        .withColumn("cpu_hours", F.col("cpu_hours").cast("double"))
        .withColumn("storage_gb_hours", F.col("storage_gb_hours").cast("double"))
        .withColumn("genai_tokens_total", F.col("genai_tokens_total").cast("long"))
    )

    # Left join streaming features with static stats
    joined = df_features.join(service_stats, on="service", how="left")

    # Calculate z-score and anomaly z-score flag (threshold abs(z_score) > 3.0)
    result = (
        joined
        .withColumn(
            "z_score",
            F.when(
                (F.col("service_stddev_cost").isNotNull()) & (F.col("service_stddev_cost") > 0.0),
                (F.col("daily_cost_usd") - F.col("service_mean_cost")) / F.col("service_stddev_cost")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "anomaly_zscore_flag",
            F.coalesce(F.abs(F.col("z_score")) > 3.0, F.lit(False))
        )
        .drop("service_mean_cost", "service_stddev_cost")
    )
    
    return result



def write_outputs(
    silver_root: Path,
    checkpoint_root: Path,
    trigger_builder: dict,
    enriched_events: DataFrame,
    daily_features: DataFrame,
    quarantine: DataFrame,
    sample_quarantine: int,
):
    events_path = silver_root / "events_enriched"
    features_path = silver_root / "features_org_daily"
    quarantine_path = silver_root / "quarantine" / "events_quality_issues"
    quarantine_samples_path = silver_root / "quarantine" / "samples"

    events_ckp = checkpoint_root / "bronze_to_silver" / "events_enriched"
    features_ckp = checkpoint_root / "bronze_to_silver" / "features_org_daily"
    quarantine_ckp = checkpoint_root / "bronze_to_silver" / "events_quality_issues"
    samples_ckp = checkpoint_root / "bronze_to_silver" / "samples"

    q_events = (
        enriched_events.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(events_ckp))
        .partitionBy("event_date")
        .trigger(**trigger_builder)
        .start(str(events_path))
    )
    q_features = (
        daily_features.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(features_ckp))
        .partitionBy("event_date")
        .trigger(**trigger_builder)
        .start(str(features_path))
    )
    q_quarantine = (
        quarantine.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(quarantine_ckp))
        .partitionBy("event_date")
        .trigger(**trigger_builder)
        .start(str(quarantine_path))
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
    )
    q_samples = (
        sample.writeStream.format("json")
        .outputMode("append")
        .option("checkpointLocation", str(samples_ckp))
        .trigger(**trigger_builder)
        .start(str(quarantine_samples_path))
    )

    return [q_events, q_features, q_quarantine, q_samples]


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
    ensure_dir(args.checkpoint_root)

    started_at = utc_now_iso()
    spark = build_spark("bronze_to_silver")

    try:
        usage, customers, users, resources = load_bronze_inputs(
            spark=spark,
            bronze_root=args.bronze_root,
            max_files_per_trigger=args.max_files_per_trigger,
        )
        prepared = prepare_events(usage)

        flagged = apply_quality_rules(prepared)
        silver_events, quarantine = split_silver_and_quarantine(flagged)
        deduped_silver_events = (
            silver_events.withWatermark("event_ts", args.watermark_delay)
            .dropDuplicates(["event_id"])
        )

        enriched = enrich_with_master(deduped_silver_events, customers, users, resources)
        features = build_daily_features(enriched, args.watermark_delay)

        trigger_builder = {"availableNow": True} if not args.continuous else {"processingTime": "30 seconds"}

        queries = write_outputs(
            silver_root=args.silver_root,
            checkpoint_root=args.checkpoint_root,
            trigger_builder=trigger_builder,
            enriched_events=enriched,
            daily_features=features,
            quarantine=quarantine,
            sample_quarantine=args.sample_quarantine,
        )

        print(f"[INFO] Bronze stream path: {args.bronze_root / 'streaming' / 'usage_events'}")
        print(f"[INFO] Silver root: {args.silver_root}")
        print(f"[INFO] Checkpoint root: {args.checkpoint_root / 'bronze_to_silver'}")
        print(f"[INFO] Watermark delay: {args.watermark_delay}")
        print(
            "[INFO] Running mode: "
            + ("continuous" if args.continuous else "availableNow micro-batches")
        )

        for query in queries:
            query.awaitTermination()

        if args.continuous:
            return 0

        print_verify_counts(spark, args.silver_root)

        usage_path = args.bronze_root / "streaming" / "usage_events"
        events_path = args.silver_root / "events_enriched"
        features_path = args.silver_root / "features_org_daily"
        quarantine_path = args.silver_root / "quarantine" / "events_quality_issues"

        records_bronze_input = spark.read.parquet(str(usage_path)).count()
        records_after_event_dedupe = (
            spark.read.parquet(str(events_path)).select("event_id").dropDuplicates(["event_id"]).count()
            if has_parquet_data(events_path)
            else 0
        )
        records_quarantine = (
            spark.read.parquet(str(quarantine_path)).count()
            if has_parquet_data(quarantine_path)
            else 0
        )
        records_silver_events = (
            spark.read.parquet(str(events_path)).count()
            if has_parquet_data(events_path)
            else 0
        )
        records_silver_features = (
            spark.read.parquet(str(features_path)).count()
            if has_parquet_data(features_path)
            else 0
        )

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
        print(f"[INFO] Quarantine records: {records_quarantine}")
        print(f"[INFO] Silver events: {records_silver_events}")
        print(f"[INFO] Silver features: {records_silver_features}")
        print(f"[OK] Silver pipeline completed. Manifest: {args.silver_root / '_control' / 'manifest.json'}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
