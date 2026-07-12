#!/usr/bin/env python3
"""Streaming Gold marts -> Serving Cassandra/AstraDB."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))

from serving_cassandra_common import (
    SCHEMAS,
    STREAMING_TABLES,
    add_common_arguments,
    build_spark_session,
    ensure_gold_path,
    initialize_serving,
    prepare_args,
    write_dataframe_to_cassandra,
)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load streaming Gold marts into Cassandra/AstraDB.")
    add_common_arguments(parser, STREAMING_TABLES)
    parser.set_defaults(table="org_daily_usage_by_service")
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=Path("datalake") / "checkpoints",
        help="Checkpoint root for streaming serving query.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep streaming query running continuously. Default drains backlog with availableNow micro-batches.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    prepared_args = prepare_args(parse_args(argv))
    if isinstance(prepared_args, int):
        return prepared_args
    args = prepared_args

    spark = build_spark_session("streaming_gold_to_serving_cassandra")
    gold_path = ensure_gold_path(args.gold_root, args.table)

    try:
        if not args.write_serving:
            print("[INFO] Dry-run mode (no Cassandra write). Checking Gold schema...")
            if any(gold_path.rglob("*.parquet")):
                df = spark.read.schema(SCHEMAS[args.table]).parquet(str(gold_path))
                print(f"[INFO] Dry-run: read {df.count()} records for table {args.table}")
            return 0

        row_writer = initialize_serving(args)
        if row_writer is None:
            return 2

        print(f"[INFO] Cassandra write mode: {args.write_mode}")
        print(f"[INFO] Table '{args.table}' is STREAMING. Starting serving stream...")
        gold_stream_df = spark.readStream.schema(SCHEMAS[args.table]).parquet(str(gold_path))

        def write_micro_batch(batch_df, batch_id):
            write_dataframe_to_cassandra(
                batch_df,
                row_writer,
                args.write_mode,
                f"micro-batch {batch_id}",
            )

        mode_str = "astradb" if args.astradb_bundle else "local"
        checkpoint_dir = args.checkpoint_root / f"gold_to_serving_{mode_str}" / args.table
        trigger_builder = {"availableNow": True} if not args.continuous else {"processingTime": "30 seconds"}

        print(f"[INFO] Starting serving stream write to Cassandra Keyspace: '{args.keyspace}' Table: '{args.table}'")
        query = (
            gold_stream_df.writeStream
            .foreachBatch(write_micro_batch)
            .option("checkpointLocation", str(checkpoint_dir))
            .trigger(**trigger_builder)
            .start()
        )
        query.awaitTermination()
        print(f"[OK] Serving stream for '{args.table}' completed successfully.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    sys.exit(main())
