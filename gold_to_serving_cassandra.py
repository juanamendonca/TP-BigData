#!/usr/bin/env python3
"""
Gold -> Serving (Cassandra/AstraDB) (point 5).

This script:
- Reads Gold mart org_daily_usage_by_service.
- Generates CQL DDL and sample queries (query-first).
- Optionally writes rows to Cassandra using cassandra-driver.
"""

from __future__ import annotations

import argparse
import socket
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Gold mart into Cassandra/AstraDB.")
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=Path("datalake") / "gold",
        help="Gold root path.",
    )
    parser.add_argument(
        "--cql-dir",
        type=Path,
        default=Path("cql"),
        help="Directory where CQL files are generated.",
    )
    parser.add_argument("--keyspace", type=str, default="finops", help="Cassandra keyspace.")
    parser.add_argument(
        "--table",
        type=str,
        default="org_daily_usage_by_service",
        help="Cassandra table name.",
    )

    parser.add_argument("--host", type=str, default=None, help="Cassandra host.")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra native transport port.")
    parser.add_argument("--username", type=str, default=None, help="Cassandra username.")
    parser.add_argument("--password", type=str, default=None, help="Cassandra password.")
    parser.add_argument(
        "--write-serving",
        action="store_true",
        help="If set, write Gold mart rows into Cassandra.",
    )
    return parser.parse_args(argv)


def generate_cql_files(cql_dir: Path, keyspace: str, table: str) -> None:
    cql_dir.mkdir(parents=True, exist_ok=True)

    ddl = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};

CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
    org_id text,
    month_bucket text,
    event_date date,
    service text,
    daily_cost_usd double,
    requests bigint,
    genai_tokens_total bigint,
    carbon_kg_total double,
    events_count bigint,
    anomaly_events_count bigint,
    quality_score double,
    PRIMARY KEY ((org_id, month_bucket), event_date, service)
) WITH CLUSTERING ORDER BY (event_date DESC, service ASC);
""".strip() + "\n"

    queries = f"""
-- Query #1: Daily usage/cost by service for one org in a month bucket
SELECT *
FROM {keyspace}.{table}
WHERE org_id = 'org_001' AND month_bucket = '2025-08';

-- Query #2: Specific day + service lookup (drill-down)
SELECT *
FROM {keyspace}.{table}
WHERE org_id = 'org_001' AND month_bucket = '2025-08' AND event_date = '2025-08-15' AND service = 'compute';
""".strip() + "\n"

    (cql_dir / "01_schema_finops.cql").write_text(ddl, encoding="utf-8")
    (cql_dir / "02_queries_finops.cql").write_text(queries, encoding="utf-8")


def load_gold_dataframe(gold_root: Path):
    spark = SparkSession.builder.appName("gold_to_serving_cassandra").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df = (
        spark.read.parquet(str(gold_root / "org_daily_usage_by_service"))
        .select(
            "org_id",
            "month_bucket",
            "event_date",
            "service",
            "daily_cost_usd",
            "requests",
            "genai_tokens_total",
            "carbon_kg_total",
            "events_count",
            "anomaly_events_count",
            "quality_score",
        )
        .where(F.col("org_id").isNotNull() & F.col("month_bucket").isNotNull() & F.col("event_date").isNotNull())
    )
    return spark, df


def is_port_open(host: str, port: int, timeout_sec: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except OSError:
        return False


def write_to_cassandra(df, host: str, port: int, username: Optional[str], password: Optional[str], keyspace: str, table: str) -> int:
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
    except Exception as exc:
        raise RuntimeError(
            "cassandra-driver is required for --write-serving. Install with: pip install cassandra-driver"
        ) from exc

    auth_provider = None
    if username and password:
        auth_provider = PlainTextAuthProvider(username=username, password=password)

    cluster = Cluster([host], port=port, auth_provider=auth_provider)
    session = cluster.connect()

    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )

    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
            org_id text,
            month_bucket text,
            event_date date,
            service text,
            daily_cost_usd double,
            requests bigint,
            genai_tokens_total bigint,
            carbon_kg_total double,
            events_count bigint,
            anomaly_events_count bigint,
            quality_score double,
            PRIMARY KEY ((org_id, month_bucket), event_date, service)
        ) WITH CLUSTERING ORDER BY (event_date DESC, service ASC)
        """
    )

    prepared = session.prepare(
        f"""
        INSERT INTO {keyspace}.{table}
        (org_id, month_bucket, event_date, service, daily_cost_usd, requests,
         genai_tokens_total, carbon_kg_total, events_count, anomaly_events_count, quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    rows_written = 0
    for row in df.toLocalIterator():
        session.execute(
            prepared,
            (
                row["org_id"],
                row["month_bucket"],
                row["event_date"],
                row["service"],
                float(row["daily_cost_usd"]) if row["daily_cost_usd"] is not None else None,
                int(row["requests"]) if row["requests"] is not None else None,
                int(row["genai_tokens_total"]) if row["genai_tokens_total"] is not None else None,
                float(row["carbon_kg_total"]) if row["carbon_kg_total"] is not None else None,
                int(row["events_count"]) if row["events_count"] is not None else None,
                int(row["anomaly_events_count"]) if row["anomaly_events_count"] is not None else None,
                float(row["quality_score"]) if row["quality_score"] is not None else None,
            ),
        )
        rows_written += 1

    session.shutdown()
    cluster.shutdown()
    return rows_written


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    generate_cql_files(args.cql_dir, args.keyspace, args.table)
    print(f"[INFO] CQL generated: {args.cql_dir / '01_schema_finops.cql'}")
    print(f"[INFO] CQL generated: {args.cql_dir / '02_queries_finops.cql'}")

    spark, gold_df = load_gold_dataframe(args.gold_root)
    try:
        total_rows = gold_df.count()
        print(f"[INFO] Gold rows ready for serving: {total_rows}")

        if not args.write_serving:
            print("[INFO] Dry-run mode (no Cassandra write). Use --write-serving to load data.")
            return 0

        if not args.host:
            raise ValueError("--host is required when --write-serving is enabled.")

        if not is_port_open(args.host, args.port):
            print(
                "[ERROR] Cannot connect to Cassandra endpoint "
                f"{args.host}:{args.port}. Connection refused or host unreachable."
            )
            print("[HINT] If using local Cassandra, start it before running this command.")
            print("[HINT] If using Docker in WSL, publish 9042 and verify with: nc -zv 127.0.0.1 9042")
            print("[HINT] If using AstraDB, provide valid --host, --username and --password.")
            return 2

        try:
            rows_written = write_to_cassandra(
                df=gold_df,
                host=args.host,
                port=args.port,
                username=args.username,
                password=args.password,
                keyspace=args.keyspace,
                table=args.table,
            )
        except Exception as exc:
            print(f"[ERROR] Failed writing to Cassandra: {exc}")
            print("[HINT] Check connectivity, credentials, keyspace permissions, and table schema.")
            return 2
        print(f"[OK] Rows written to Cassandra: {rows_written}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
