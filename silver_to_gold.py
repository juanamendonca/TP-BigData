#!/usr/bin/env python3
"""
Silver -> Gold pipeline (Structured Streaming) (point 4).
Builds FinOps mart: org_daily_usage_by_service.
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


SILVER_FEATURES_SCHEMA = T.StructType(
    [
        T.StructField("org_id", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("daily_cost_usd", T.DoubleType(), True),
        T.StructField("requests", T.LongType(), True),
        T.StructField("genai_tokens_total", T.LongType(), True),
        T.StructField("carbon_kg_total", T.DoubleType(), True),
        T.StructField("events_count", T.LongType(), True),
        T.StructField("anomaly_events_count", T.LongType(), True),
        T.StructField("event_date", T.DateType(), True),
    ]
)


@dataclass
class GoldStats:
    records_silver_input: int
    records_gold_output: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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
    parser = argparse.ArgumentParser(description="Build Gold FinOps mart from Silver.")
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
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=Path("datalake") / "checkpoints",
        help="Checkpoint root for streaming queries.",
    )
    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=50,
        help="Max Silver Parquet files per micro-batch.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep streaming query running continuously. Default drains backlog with availableNow.",
    )
    return parser.parse_args(argv)


def has_parquet_data(path: Path) -> bool:
    if not path.exists():
        return False
    return any(path.rglob("*.parquet"))


def load_silver_features(
    spark: SparkSession,
    silver_root: Path,
    max_files_per_trigger: int,
) -> DataFrame:
    return (
        spark.readStream.format("parquet")
        .schema(SILVER_FEATURES_SCHEMA)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(str(silver_root / "features_org_daily"))
    )


def build_gold_mart(features: DataFrame) -> DataFrame:
    # Gold mart keeps daily grain per org/service and adds serving-friendly bucket.
    return (
        features.select(
            F.col("event_date"),
            F.col("org_id"),
            F.col("service"),
            F.round(F.col("daily_cost_usd"), 6).alias("daily_cost_usd"),
            F.col("requests").cast("long").alias("requests"),
            F.col("genai_tokens_total").cast("long").alias("genai_tokens_total"),
            F.round(F.col("carbon_kg_total"), 6).alias("carbon_kg_total"),
            F.col("events_count").cast("long").alias("events_count"),
            F.col("anomaly_events_count").cast("long").alias("anomaly_events_count"),
        )
        .withColumn("month_bucket", F.date_format(F.col("event_date"), "yyyy-MM"))
        .withColumn(
            "quality_score",
            F.when(F.col("events_count") > 0, 1.0 - (F.col("anomaly_events_count") / F.col("events_count"))).otherwise(F.lit(1.0)),
        )
    )


def write_gold(
    gold_root: Path,
    checkpoint_root: Path,
    mart: DataFrame,
    trigger_builder: dict,
):
    out_path = gold_root / "org_daily_usage_by_service"
    checkpoint_dir = checkpoint_root / "silver_to_gold" / "org_daily_usage_by_service"
    return (
        mart.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_dir))
        .partitionBy("event_date")
        .trigger(**trigger_builder)
        .start(str(out_path))
    )


def write_manifest(gold_root: Path, stats: GoldStats, started_at: str, finished_at: str) -> None:
    control_dir = gold_root / "_control"
    ensure_dir(control_dir)

    manifest = {
        "pipeline": "silver_to_gold",
        "gold_mart": "org_daily_usage_by_service",
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "stats": asdict(stats),
        "output_path": "datalake/gold/org_daily_usage_by_service",
        "grain": "event_date, org_id, service",
        "metrics": [
            "daily_cost_usd",
            "requests",
            "genai_tokens_total",
            "carbon_kg_total",
            "events_count",
            "anomaly_events_count",
            "quality_score",
        ],
    }

    with (control_dir / "manifest.json").open("w", encoding="utf-8") as w:
        json.dump(manifest, w, indent=2, ensure_ascii=False)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    ensure_dir(args.gold_root)
    ensure_dir(args.checkpoint_root)

    started_at = utc_now_iso()
    spark = build_spark("silver_to_gold")

    try:
        features = load_silver_features(
            spark=spark,
            silver_root=args.silver_root,
            max_files_per_trigger=args.max_files_per_trigger,
        )
        mart = build_gold_mart(features)

        trigger_builder = {"availableNow": True} if not args.continuous else {"processingTime": "30 seconds"}

        query = write_gold(
            gold_root=args.gold_root,
            checkpoint_root=args.checkpoint_root,
            mart=mart,
            trigger_builder=trigger_builder,
        )
        print(f"[INFO] Silver stream path: {args.silver_root / 'features_org_daily'}")
        print(f"[INFO] Gold mart path: {args.gold_root / 'org_daily_usage_by_service'}")
        print(f"[INFO] Checkpoint root: {args.checkpoint_root / 'silver_to_gold'}")
        print(
            "[INFO] Running mode: "
            + ("continuous" if args.continuous else "availableNow micro-batches")
        )
        query.awaitTermination()

        if args.continuous:
            return 0

        silver_path = args.silver_root / "features_org_daily"
        gold_path = args.gold_root / "org_daily_usage_by_service"
        stats = GoldStats(
            records_silver_input=spark.read.parquet(str(silver_path)).count()
            if has_parquet_data(silver_path)
            else 0,
            records_gold_output=spark.read.parquet(str(gold_path)).count()
            if has_parquet_data(gold_path)
            else 0,
        )

        finished_at = utc_now_iso()
        write_manifest(args.gold_root, stats, started_at, finished_at)

        print(f"[INFO] Silver input records: {stats.records_silver_input}")
        print(f"[INFO] Gold output records: {stats.records_gold_output}")
        print("[OK] Gold mart completed. Manifest: datalake/gold/_control/manifest.json")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
