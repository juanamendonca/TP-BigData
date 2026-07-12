#!/usr/bin/env python3
"""
Run the batch flow end-to-end:

Landing CSV -> Bronze -> Silver -> Gold -> Cassandra serving.

After loading Cassandra, runs verification queries for:
- revenue_by_org_month
- tickets_by_org_date
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


BATCH_SERVING_TABLES = [
    "revenue_by_org_month",
    "tickets_by_org_date",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch pipeline and Cassandra verification queries.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config") / "cassandra_config.json",
        help="Cassandra/AstraDB config JSON.",
    )
    parser.add_argument(
        "--batch-date",
        type=str,
        default="2026-06-15",
        help="Batch partition date passed to batch_landing_to_bronze.py in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--landing-root",
        type=Path,
        default=Path("datalake") / "landing",
        help="Input landing directory.",
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
        "--gold-root",
        type=Path,
        default=Path("datalake") / "gold",
        help="Gold root path.",
    )
    parser.add_argument(
        "--write-mode",
        choices=["driver", "executor"],
        default="driver",
        help="Cassandra write mode passed to batch_gold_to_serving_cassandra.py.",
    )
    parser.add_argument("--org-id", type=str, default="org_xaji0y6d")
    parser.add_argument("--month-bucket", type=str, default="2025-07")
    parser.add_argument("--date-from", type=str, default="2025-07-01")
    parser.add_argument("--date-to", type=str, default="2025-07-31")
    return parser.parse_args()


def run_step(command: list[str], env: dict[str, str]) -> None:
    print("\n[RUN] " + " ".join(command), flush=True)
    subprocess.run(command, check=True, env=env)


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_cluster(cfg: dict[str, Any]):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    mode = cfg.get("mode", "local")
    section = cfg[mode]
    keyspace = section.get("keyspace", "finops")

    if mode == "astradb":
        auth_provider = PlainTextAuthProvider("token", section["token"])
        cluster = Cluster(
            cloud={"secure_connect_bundle": section["bundle"]},
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


def run_verification_queries(cfg: dict[str, Any], args: argparse.Namespace) -> None:
    cluster, keyspace = build_cluster(cfg)
    session = cluster.connect(keyspace)

    try:
        print_rows(
            "revenue_by_org_month",
            session.execute(
                """
                SELECT org_id, month, revenue_usd, credits_usd, taxes_usd, net_revenue_usd, fx_applied
                FROM revenue_by_org_month
                WHERE org_id = %s
                """,
                (args.org_id,),
            ),
        )

        print_rows(
            "tickets_by_org_date",
            session.execute(
                """
                SELECT event_date, ticket_count, sla_breach_rate, csat_avg, severity_breakdown
                FROM tickets_by_org_date
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
    args = parse_args()
    if not args.config.exists():
        print(f"[ERROR] Config not found: {args.config}", file=sys.stderr)
        return 2

    env = os.environ.copy()
    python = sys.executable

    run_step(
        [
            python,
            "batch_landing_to_bronze.py",
            "--landing-root",
            str(args.landing_root),
            "--bronze-root",
            str(args.bronze_root),
            "--batch-date",
            args.batch_date,
        ],
        env,
    )
    run_step(
        [
            python,
            "batch_bronze_to_silver.py",
            "--bronze-root",
            str(args.bronze_root),
            "--silver-root",
            str(args.silver_root),
        ],
        env,
    )
    run_step(
        [
            python,
            "batch_silver_to_gold.py",
            "--silver-root",
            str(args.silver_root),
            "--gold-root",
            str(args.gold_root),
        ],
        env,
    )

    for table in BATCH_SERVING_TABLES:
        run_step(
            [
                python,
                "batch_gold_to_serving_cassandra.py",
                "--write-serving",
                "--config",
                str(args.config),
                "--gold-root",
                str(args.gold_root),
                "--table",
                table,
                "--write-mode",
                args.write_mode,
            ],
            env,
        )

    run_verification_queries(load_config(args.config), args)
    print("\n[OK] Batch flow completed end-to-end.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
