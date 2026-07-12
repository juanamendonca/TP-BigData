#!/usr/bin/env python3
"""Batch Gold marts -> Serving Cassandra/AstraDB."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))

from serving_cassandra_common import (
    BATCH_TABLES,
    SCHEMAS,
    add_common_arguments,
    build_spark_session,
    ensure_gold_path,
    initialize_serving,
    prepare_args,
    write_dataframe_to_cassandra,
)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load batch Gold marts into Cassandra/AstraDB.")
    add_common_arguments(parser, BATCH_TABLES)
    parser.set_defaults(table="revenue_by_org_month")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    prepared_args = prepare_args(parse_args(argv))
    if isinstance(prepared_args, int):
        return prepared_args
    args = prepared_args

    spark = build_spark_session("batch_gold_to_serving_cassandra")
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
        print(f"[INFO] Table '{args.table}' is BATCH. Reading Gold parquet statically...")
        gold_df = spark.read.schema(SCHEMAS[args.table]).parquet(str(gold_path))
        write_dataframe_to_cassandra(gold_df, row_writer, args.write_mode, f"static table '{args.table}'")
        print(f"[OK] Batch load to Cassandra table '{args.table}' completed successfully.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    sys.exit(main())
