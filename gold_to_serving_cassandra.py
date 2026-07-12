#!/usr/bin/env python3
"""Compatibility wrapper for Gold -> Serving Cassandra/AstraDB loaders."""

from __future__ import annotations

import argparse
import sys
from typing import Optional

import batch_gold_to_serving_cassandra
import streaming_gold_to_serving_cassandra
from serving_cassandra_common import BATCH_TABLES, SCHEMAS, STREAMING_TABLES


def _selected_table(argv: Optional[list[str]]) -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--table", choices=list(SCHEMAS.keys()), default="org_daily_usage_by_service")
    args, _ = parser.parse_known_args(argv)
    return args.table


def _strip_streaming_only_args(argv: Optional[list[str]]) -> Optional[list[str]]:
    if argv is None:
        return None

    stripped: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            continue
        if arg == "--continuous":
            continue
        if arg == "--checkpoint-root":
            skip_next = True
            continue
        if arg.startswith("--checkpoint-root="):
            continue
        stripped.append(arg)
    return stripped


def main(argv: Optional[list[str]] = None) -> int:
    effective_argv = sys.argv[1:] if argv is None else argv
    table = _selected_table(effective_argv)
    if table in BATCH_TABLES:
        return batch_gold_to_serving_cassandra.main(_strip_streaming_only_args(effective_argv))
    if table in STREAMING_TABLES:
        return streaming_gold_to_serving_cassandra.main(effective_argv)

    print(f"[ERROR] Unsupported table: {table}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
