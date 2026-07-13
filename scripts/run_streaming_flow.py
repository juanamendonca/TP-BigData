#!/usr/bin/env python3
"""
Run the streaming flow end-to-end:

Landing JSONL -> Bronze -> Silver -> Gold -> Cassandra serving.

After loading Cassandra, runs verification queries for:
- org_daily_usage_by_service
- cost_anomaly_mart
- genai_tokens_by_org_date
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


STREAMING_SERVING_TABLES = [
    "org_daily_usage_by_service",
    "cost_anomaly_mart",
    "genai_tokens_by_org_date",
]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STREAMING_FLOW_DIR = Path(__file__).resolve().parent / "streaming-flow"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run streaming pipeline and Cassandra verification queries.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config") / "cassandra_config.json",
        help="Cassandra/AstraDB config JSON.",
    )
    parser.add_argument(
        "--watermark-delay",
        type=str,
        default="2 days",
        help="Watermark delay used by streaming pipelines.",
    )
    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=100,
        help="Max files per micro-batch for streaming file sources.",
    )
    parser.add_argument(
        "--write-mode",
        choices=["driver", "executor"],
        default="driver",
        help="Cassandra write mode passed to streaming_gold_to_serving_cassandra.py.",
    )
    parser.add_argument("--org-id", type=str, default="org_xaji0y6d")
    parser.add_argument("--month-bucket", type=str, default="2025-07")
    parser.add_argument("--date-from", type=str, default="2025-07-01")
    parser.add_argument("--date-to", type=str, default="2025-07-31")
    return parser.parse_args()


def run_step(command: list[str], env: dict[str, str]) -> None:
    print("\n[RUN] " + " ".join(command), flush=True)
    subprocess.run(command, check=True, env=env, cwd=PROJECT_ROOT)


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_cluster(cfg: dict[str, Any], config_path: Path | None = None):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    mode = cfg.get("mode", "local")
    section = cfg[mode]
    keyspace = section.get("keyspace", "finops")

    if mode == "astradb":
        bundle_val = section["bundle"]
        if config_path:
            bundle_path = Path(bundle_val)
            if not bundle_path.exists():
                sibling_path = config_path.parent / bundle_val
                if sibling_path.exists():
                    bundle_val = str(sibling_path.resolve())
        auth_provider = PlainTextAuthProvider("token", section["token"])
        cluster = Cluster(
            cloud={"secure_connect_bundle": bundle_val},
            auth_provider=auth_provider,
        )
    else:
        cluster = Cluster([section.get("host", "127.0.0.1")], port=section.get("port", 9042))

    return cluster, keyspace


def format_value(value: Any) -> str:
    return "" if value is None else str(value)


def print_rows(label: str, rows, limit: int = 20) -> None:
    print(f"\n[QUERY] {label}")
    buffered_rows = []
    total_count = 0
    for row in rows:
        total_count += 1
        if len(buffered_rows) < limit:
            buffered_rows.append(row)

    if not buffered_rows:
        print("(no rows)")
        print(f"[OK] {label}: rows=0")
        return

    columns = list(buffered_rows[0]._fields)
    rendered_rows = [[format_value(getattr(row, col)) for col in columns] for row in buffered_rows]
    widths = [
        max(len(col), *(len(rendered_row[idx]) for rendered_row in rendered_rows))
        for idx, col in enumerate(columns)
    ]

    header = " | ".join(col.ljust(widths[idx]) for idx, col in enumerate(columns))
    separator = "-+-".join("-" * width for width in widths)
    print(header)
    print(separator)
    for rendered_row in rendered_rows:
        print(" | ".join(value.ljust(widths[idx]) for idx, value in enumerate(rendered_row)))

    if total_count > limit:
        print(f"... {total_count - limit} more rows")
    print(f"[OK] {label}: rows={total_count}")


def run_verification_queries(cfg: dict[str, Any], config_path: Path, args: argparse.Namespace) -> None:
    cluster, keyspace = build_cluster(cfg, config_path)
    session = cluster.connect(keyspace)

    try:
        print_rows(
            "org_daily_usage_by_service",
            session.execute(
                """
                SELECT org_id, month_bucket, event_date, service, daily_cost_usd, requests
                FROM org_daily_usage_by_service
                WHERE org_id = %s
                  AND month_bucket = %s
                  AND event_date >= %s AND event_date <= %s
                """,
                (args.org_id, args.month_bucket, args.date_from, args.date_to),
            ),
        )

        print_rows(
            "cost_anomaly_mart",
            session.execute(
                """
                SELECT org_id, month_bucket, event_date, service,
                       anomaly_events_count, events_count, quality_score,
                       z_score, anomaly_zscore_flag
                FROM cost_anomaly_mart
                WHERE org_id = %s
                  AND month_bucket = %s
                  AND event_date >= %s AND event_date <= %s
                """,
                (args.org_id, args.month_bucket, args.date_from, args.date_to),
            ),
        )

        print_rows(
            "genai_tokens_by_org_date",
            session.execute(
                """
                SELECT org_id, month_bucket, event_date, genai_tokens_total, estimated_cost_usd
                FROM genai_tokens_by_org_date
                WHERE org_id = %s
                  AND month_bucket = %s
                  AND event_date >= %s AND event_date <= %s
                """,
                (args.org_id, args.month_bucket, args.date_from, args.date_to),
            ),
        )
    finally:
        session.shutdown()
        cluster.shutdown()


def main() -> int:
    os.chdir(PROJECT_ROOT)
    args = parse_args()
    config_path = args.config if args.config.is_absolute() else PROJECT_ROOT / args.config
    if not config_path.exists():
        print(f"[ERROR] Config not found: {args.config}", file=sys.stderr)
        return 2

    env = os.environ.copy()
    python = sys.executable

    run_step(
        [
            python,
            str(STREAMING_FLOW_DIR / "streaming_landing_to_bronze.py"),
            "--watermark-delay",
            args.watermark_delay,
            "--max-files-per-trigger",
            str(args.max_files_per_trigger),
        ],
        env,
    )
    run_step(
        [
            python,
            str(STREAMING_FLOW_DIR / "streaming_bronze_to_silver.py"),
            "--watermark-delay",
            args.watermark_delay,
            "--max-files-per-trigger",
            str(args.max_files_per_trigger),
        ],
        env,
    )
    run_step(
        [
            python,
            str(STREAMING_FLOW_DIR / "streaming_silver_to_gold.py"),
            "--max-files-per-trigger",
            str(args.max_files_per_trigger),
        ],
        env,
    )

    for table in STREAMING_SERVING_TABLES:
        run_step(
            [
                python,
                str(STREAMING_FLOW_DIR / "streaming_gold_to_serving_cassandra.py"),
                "--write-serving",
                "--config",
                str(config_path),
                "--table",
                table,
                "--write-mode",
                args.write_mode,
            ],
            env,
        )

    run_verification_queries(load_config(config_path), config_path, args)
    print("\n[OK] Streaming flow completed end-to-end.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
