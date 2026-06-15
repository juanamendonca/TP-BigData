#!/usr/bin/env python3
"""
Streaming ingestion pipeline: landing usage_events_stream -> bronze Parquet (PySpark).

Aligned with second partial + first partial (streaming Bronze):
- Structured Streaming over usage_events_stream/*.jsonl
- Explicit schema (v1 + v2 fields)
- withWatermark + dropDuplicates(event_id)
- Late data handling (quarantine path)
- Checkpointing enabled
- Technical columns ingest_ts and source_file
- Partitioned Parquet output by event_date
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


EVENT_SCHEMA = T.StructType(
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
    ]
)


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
    return spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Streaming load from landing usage_events_stream to bronze parquet with "
            "watermark, dedupe, late data handling and checkpoints."
        )
    )
    parser.add_argument(
        "--input-path",
        type=Path,
        default=Path("datalake") / "landing" / "usage_events_stream",
        help="Directory with usage_events_stream JSONL files.",
    )
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=Path("datalake") / "bronze",
        help="Bronze root output path.",
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
        help="Watermark delay for event time dedupe (example: '2 days', '12 hours').",
    )
    parser.add_argument(
        "--late-threshold-hours",
        type=int,
        default=48,
        help="Events older than this threshold are flagged as late and sent to quarantine.",
    )
    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=50,
        help="Max source files per micro-batch.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep query running continuously. Default runs trigger once and exits.",
    )
    return parser.parse_args()


def read_usage_stream(spark: SparkSession, input_path: Path, max_files_per_trigger: int) -> DataFrame:
    return (
        spark.readStream.format("json")
        .schema(EVENT_SCHEMA)
        .option("pathGlobFilter", "*.jsonl")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(str(input_path))
    )


def add_technical_columns(df: DataFrame, late_threshold_hours: int) -> DataFrame:
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("batch_date", F.to_date("ingest_ts"))
        .withColumn("value_num", F.col("value").cast("double"))
        .withColumn(
            "is_late",
            F.col("event_ts")
            < F.expr(f"current_timestamp() - INTERVAL {late_threshold_hours} HOURS"),
        )
    )


def build_queries(df: DataFrame, args: argparse.Namespace):
    bronze_stream_path = args.bronze_root / "streaming" / "usage_events"
    invalid_path = args.bronze_root / "quarantine" / "usage_events_invalid"
    late_path = args.bronze_root / "quarantine" / "usage_events_late"

    bronze_ckp = args.checkpoint_root / "streaming_landing_to_bronze" / "usage_events"
    invalid_ckp = args.checkpoint_root / "streaming_landing_to_bronze" / "usage_events_invalid"
    late_ckp = args.checkpoint_root / "streaming_landing_to_bronze" / "usage_events_late"

    trigger_builder = {"once": True} if not args.continuous else {"processingTime": "30 seconds"}

    valid_events = (
        df.filter(F.col("_corrupt_record").isNull())
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
    )

    invalid_events = df.filter(
        F.col("_corrupt_record").isNotNull()
        | F.col("event_id").isNull()
        | F.col("event_ts").isNull()
    )

    deduped = valid_events.withWatermark("event_ts", args.watermark_delay).dropDuplicates(
        ["event_id"]
    )

    on_time = deduped.filter(~F.col("is_late"))
    late_events = deduped.filter(F.col("is_late"))

    q_bronze = (
        on_time.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(bronze_ckp))
        .partitionBy("event_date")
        .trigger(**trigger_builder)
        .start(str(bronze_stream_path))
    )

    q_invalid = (
        invalid_events.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(invalid_ckp))
        .partitionBy("batch_date")
        .trigger(**trigger_builder)
        .start(str(invalid_path))
    )

    q_late = (
        late_events.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(late_ckp))
        .partitionBy("event_date")
        .trigger(**trigger_builder)
        .start(str(late_path))
    )

    return [q_bronze, q_invalid, q_late]


def main() -> int:
    args = parse_args()

    if not args.input_path.exists():
        raise FileNotFoundError(f"Input path not found: {args.input_path}")

    args.bronze_root.mkdir(parents=True, exist_ok=True)
    args.checkpoint_root.mkdir(parents=True, exist_ok=True)

    spark = build_spark("streaming_landing_to_bronze")

    try:
        source_df = read_usage_stream(
            spark=spark,
            input_path=args.input_path,
            max_files_per_trigger=args.max_files_per_trigger,
        )
        staged_df = add_technical_columns(source_df, args.late_threshold_hours)
        queries = build_queries(staged_df, args)

        print(f"[INFO] Input stream: {args.input_path}")
        print(f"[INFO] Bronze stream path: {args.bronze_root / 'streaming' / 'usage_events'}")
        print(f"[INFO] Checkpoint root: {args.checkpoint_root}")
        print(
            "[INFO] Running mode: "
            + ("continuous" if args.continuous else "trigger once (batch-like)")
        )

        for query in queries:
            query.awaitTermination()

        print("[OK] Streaming -> Bronze completed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
