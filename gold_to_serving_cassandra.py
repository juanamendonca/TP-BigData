#!/usr/bin/env python3
"""
Gold -> Serving (Cassandra/AstraDB).

This script:
- Supports 5 Gold marts (both batch and streaming grain).
- Generates CQL DDL and 5 analytical queries.
- Runs schema initialization (DDL) for all tables on the Driver.
- Writes streaming marts using Spark Structured Streaming + foreachBatch.
- Writes batch marts using static Spark read.
- Supports driver writes for local stability and executor writes for larger loads.
- Supports AstraDB cloud and local Docker modes.
"""

from __future__ import annotations

import argparse
import socket
from functools import partial
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Spark Schemas for the 5 Gold Marts
SCHEMAS = {
    "org_daily_usage_by_service": T.StructType([
        T.StructField("event_date", T.DateType(), True),
        T.StructField("org_id", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("daily_cost_usd", T.DoubleType(), True),
        T.StructField("requests", T.LongType(), True),
        T.StructField("cpu_hours", T.DoubleType(), True),
        T.StructField("storage_gb_hours", T.DoubleType(), True),
        T.StructField("genai_tokens_total", T.LongType(), True),
        T.StructField("carbon_kg_total", T.DoubleType(), True),
        T.StructField("events_count", T.LongType(), True),
        T.StructField("anomaly_events_count", T.LongType(), True),
        T.StructField("month_bucket", T.StringType(), True),
        T.StructField("quality_score", T.DoubleType(), True),
    ]),
    "revenue_by_org_month": T.StructType([
        T.StructField("org_id", T.StringType(), True),
        T.StructField("month", T.StringType(), True),
        T.StructField("revenue_usd", T.DoubleType(), True),
        T.StructField("credits_usd", T.DoubleType(), True),
        T.StructField("taxes_usd", T.DoubleType(), True),
        T.StructField("fx_applied", T.DoubleType(), True),
        T.StructField("net_revenue_usd", T.DoubleType(), True),
    ]),
    "tickets_by_org_date": T.StructType([
        T.StructField("org_id", T.StringType(), True),
        T.StructField("event_date", T.DateType(), True),
        T.StructField("ticket_count", T.LongType(), True),
        T.StructField("sla_breach_rate", T.DoubleType(), True),
        T.StructField("csat_avg", T.DoubleType(), True),
        T.StructField("severity_breakdown", T.MapType(T.StringType(), T.LongType()), True),
        T.StructField("month_bucket", T.StringType(), True),
    ]),
    "genai_tokens_by_org_date": T.StructType([
        T.StructField("event_date", T.DateType(), True),
        T.StructField("org_id", T.StringType(), True),
        T.StructField("genai_tokens_total", T.LongType(), True),
        T.StructField("estimated_cost_usd", T.DoubleType(), True),
        T.StructField("month_bucket", T.StringType(), True),
    ]),
    "cost_anomaly_mart": T.StructType([
        T.StructField("event_date", T.DateType(), True),
        T.StructField("org_id", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("anomaly_events_count", T.LongType(), True),
        T.StructField("events_count", T.LongType(), True),
        T.StructField("quality_score", T.DoubleType(), True),
        T.StructField("z_score", T.DoubleType(), True),
        T.StructField("anomaly_zscore_flag", T.BooleanType(), True),
        T.StructField("month_bucket", T.StringType(), True),
    ]),
}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Gold marts into Cassandra/AstraDB.")
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
        choices=list(SCHEMAS.keys()),
        help="Cassandra table name to process.",
    )
    parser.add_argument("--host", type=str, default=None, help="Cassandra host (local mode).")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra native transport port (local mode).")
    parser.add_argument("--username", type=str, default=None, help="Cassandra username (local mode).")
    parser.add_argument("--password", type=str, default=None, help="Cassandra password (local mode).")
    parser.add_argument(
        "--astradb-bundle",
        type=str,
        default=None,
        help="Path to AstraDB secure connect bundle ZIP (AstraDB cloud mode).",
    )
    parser.add_argument(
        "--astradb-token",
        type=str,
        default=None,
        help="AstraDB application token starting with 'AstraCS:...' (AstraDB cloud mode).",
    )
    parser.add_argument(
        "--write-serving",
        action="store_true",
        help="If set, write Gold mart rows into Cassandra.",
    )
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
    parser.add_argument(
        "--write-mode",
        choices=["driver", "executor"],
        default="driver",
        help=(
            "Cassandra write execution mode. 'driver' is stable for local/TP-sized data; "
            "'executor' uses Spark foreachPartition for larger loads."
        ),
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to cassandra_config.json (see cassandra_config.example.json). "
             "Overrides defaults; explicit CLI args override config file.",
    )
    return parser.parse_args(argv)


def load_config(config_path: str, args: argparse.Namespace) -> argparse.Namespace:
    import json
    with open(config_path) as f:
        cfg = json.load(f)

    mode = cfg.get("mode", "local")
    section = cfg.get(mode, {})

    if mode == "astradb":
        if args.astradb_bundle is None:
            args.astradb_bundle = section.get("bundle")
        if args.astradb_token is None:
            args.astradb_token = section.get("token")
    else:
        if args.host is None:
            args.host = section.get("host", "127.0.0.1")
        if args.port == 9042:
            args.port = section.get("port", 9042)

    if args.keyspace == "finops":
        args.keyspace = section.get("keyspace", "finops")

    return args


def generate_cql_files(cql_dir: Path, keyspace: str) -> None:
    cql_dir.mkdir(parents=True, exist_ok=True)

    ddl = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};

-- 1) org_daily_usage_by_service: query por org + month + date + servicio
CREATE TABLE IF NOT EXISTS {keyspace}.org_daily_usage_by_service (
    org_id text,
    month_bucket text,
    event_date date,
    service text,
    daily_cost_usd double,
    requests bigint,
    cpu_hours double,
    storage_gb_hours double,
    genai_tokens_total bigint,
    carbon_kg_total double,
    events_count bigint,
    anomaly_events_count bigint,
    quality_score double,
    PRIMARY KEY ((org_id, month_bucket), event_date, service)
) WITH CLUSTERING ORDER BY (event_date DESC, service ASC);

-- 2) revenue_by_org_month: query por org + mes de facturacion
CREATE TABLE IF NOT EXISTS {keyspace}.revenue_by_org_month (
    org_id text,
    month text,
    revenue_usd double,
    credits_usd double,
    taxes_usd double,
    net_revenue_usd double,
    fx_applied double,
    PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

-- 3) tickets_by_org_date: query por org + month + event_date con severity breakdown en Coleccion (Map)
CREATE TABLE IF NOT EXISTS {keyspace}.tickets_by_org_date (
    org_id text,
    month_bucket text,
    event_date date,
    ticket_count bigint,
    sla_breach_rate double,
    csat_avg double,
    severity_breakdown map<text, int>,
    PRIMARY KEY ((org_id, month_bucket), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

-- 4) genai_tokens_by_org_date: query por org + month + event_date
CREATE TABLE IF NOT EXISTS {keyspace}.genai_tokens_by_org_date (
    org_id text,
    month_bucket text,
    event_date date,
    genai_tokens_total bigint,
    estimated_cost_usd double,
    PRIMARY KEY ((org_id, month_bucket), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

-- 5) cost_anomaly_mart: query por org + month + event_date + service para ver anomalias (flag y zscore)
CREATE TABLE IF NOT EXISTS {keyspace}.cost_anomaly_mart (
    org_id text,
    month_bucket text,
    event_date date,
    service text,
    anomaly_events_count bigint,
    events_count bigint,
    quality_score double,
    z_score double,
    anomaly_zscore_flag boolean,
    PRIMARY KEY ((org_id, month_bucket), event_date, service)
) WITH CLUSTERING ORDER BY (event_date DESC, service ASC);
""".strip() + "\n"

    queries = f"""
-- Consulta #1: Costos y requests diarios por org y servicio en un rango de fechas.
SELECT org_id, month_bucket, event_date, service, daily_cost_usd, requests
FROM {keyspace}.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket = '2025-07' 
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #2: Datos para Top-N servicios por costo acumulado en los últimos 14 días para una organización.
SELECT service, daily_cost_usd
FROM {keyspace}.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket IN ('2025-07', '2025-08') 
  AND event_date >= '2025-07-18' AND event_date <= '2025-07-31';

-- Consulta #3: Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
-- Permite visualizar la evolución de tickets de soporte y SLA breaches. 
-- El desglose por severidad se extrae de la colección severity_breakdown (map).
SELECT event_date, ticket_count, sla_breach_rate, csat_avg, severity_breakdown
FROM {keyspace}.tickets_by_org_date
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket = '2025-07' 
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD).
-- Muestra la facturación neta mensual consolidada de la organización.
SELECT org_id, month, revenue_usd, credits_usd, taxes_usd, net_revenue_usd, fx_applied
FROM {keyspace}.revenue_by_org_month
WHERE org_id = 'org_xaji0y6d';

-- Consulta #5: Tokens GenAI y costo estimado por día.
-- Permite monitorear el consumo de Inteligencia Artificial Generativa y sus costos asociados por organización.
SELECT event_date, genai_tokens_total, estimated_cost_usd
FROM {keyspace}.genai_tokens_by_org_date
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket = '2025-07' 
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';
""".strip() + "\n"

    (cql_dir / "01_schema_finops.cql").write_text(ddl, encoding="utf-8")
    (cql_dir / "02_queries_finops.cql").write_text(queries, encoding="utf-8")


def is_port_open(host: str, port: int, timeout_sec: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except OSError:
        return False


def _build_cluster(host, port, username, password, astradb_bundle=None, astradb_token=None):
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    if astradb_bundle:
        auth_provider = PlainTextAuthProvider("token", astradb_token)
        return Cluster(cloud={"secure_connect_bundle": astradb_bundle}, auth_provider=auth_provider)
    else:
        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
        return Cluster([host], port=port, auth_provider=auth_provider)


def initialize_cassandra_schema(host: Optional[str], port: int, username: Optional[str], password: Optional[str],
                               keyspace: str, astradb_bundle: Optional[str] = None, astradb_token: Optional[str] = None) -> None:
    cluster = _build_cluster(host, port, username, password, astradb_bundle, astradb_token)
    session = cluster.connect()

    print(f"[INFO] Initializing keyspace and tables on Cassandra...")

    if not astradb_bundle:
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )

    session.set_keyspace(keyspace)

    # 1) org_daily_usage_by_service
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
            org_id text,
            month_bucket text,
            event_date date,
            service text,
            daily_cost_usd double,
            requests bigint,
            cpu_hours double,
            storage_gb_hours double,
            genai_tokens_total bigint,
            carbon_kg_total double,
            events_count bigint,
            anomaly_events_count bigint,
            quality_score double,
            PRIMARY KEY ((org_id, month_bucket), event_date, service)
        ) WITH CLUSTERING ORDER BY (event_date DESC, service ASC)
        """
    )

    # 2) revenue_by_org_month
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS revenue_by_org_month (
            org_id text,
            month text,
            revenue_usd double,
            credits_usd double,
            taxes_usd double,
            net_revenue_usd double,
            fx_applied double,
            PRIMARY KEY ((org_id), month)
        ) WITH CLUSTERING ORDER BY (month DESC)
        """
    )

    # 3) tickets_by_org_date
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS tickets_by_org_date (
            org_id text,
            month_bucket text,
            event_date date,
            ticket_count bigint,
            sla_breach_rate double,
            csat_avg double,
            severity_breakdown map<text, int>,
            PRIMARY KEY ((org_id, month_bucket), event_date)
        ) WITH CLUSTERING ORDER BY (event_date DESC)
        """
    )

    # 4) genai_tokens_by_org_date
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
            org_id text,
            month_bucket text,
            event_date date,
            genai_tokens_total bigint,
            estimated_cost_usd double,
            PRIMARY KEY ((org_id, month_bucket), event_date)
        ) WITH CLUSTERING ORDER BY (event_date DESC)
        """
    )

    # 5) cost_anomaly_mart
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS cost_anomaly_mart (
            org_id text,
            month_bucket text,
            event_date date,
            service text,
            anomaly_events_count bigint,
            events_count bigint,
            quality_score double,
            z_score double,
            anomaly_zscore_flag boolean,
            PRIMARY KEY ((org_id, month_bucket), event_date, service)
        ) WITH CLUSTERING ORDER BY (event_date DESC, service ASC)
        """
    )

    session.shutdown()
    cluster.shutdown()


def write_cassandra_rows(rows, connection: dict) -> None:
    cluster = _build_cluster(
        connection["host"],
        connection["port"],
        connection["username"],
        connection["password"],
        connection["astradb_bundle"],
        connection["astradb_token"],
    )
    session = cluster.connect(connection["keyspace"])
    table = connection["table"]

    try:
        if table == "org_daily_usage_by_service":
            prepared = session.prepare(
                f"""
                INSERT INTO {table}
                (org_id, month_bucket, event_date, service, daily_cost_usd, requests,
                 cpu_hours, storage_gb_hours, genai_tokens_total, carbon_kg_total,
                 events_count, anomaly_events_count, quality_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
            for row in rows:
                if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
                    continue
                session.execute(
                    prepared,
                    (
                        row["org_id"],
                        row["month_bucket"],
                        row["event_date"],
                        row["service"],
                        float(row["daily_cost_usd"]) if row["daily_cost_usd"] is not None else None,
                        int(row["requests"]) if row["requests"] is not None else None,
                        float(row["cpu_hours"]) if row["cpu_hours"] is not None else None,
                        float(row["storage_gb_hours"]) if row["storage_gb_hours"] is not None else None,
                        int(row["genai_tokens_total"]) if row["genai_tokens_total"] is not None else None,
                        float(row["carbon_kg_total"]) if row["carbon_kg_total"] is not None else None,
                        int(row["events_count"]) if row["events_count"] is not None else None,
                        int(row["anomaly_events_count"]) if row["anomaly_events_count"] is not None else None,
                        float(row["quality_score"]) if row["quality_score"] is not None else None,
                    ),
                )
        elif table == "revenue_by_org_month":
            prepared = session.prepare(
                f"""
                INSERT INTO {table}
                (org_id, month, revenue_usd, credits_usd, taxes_usd, net_revenue_usd, fx_applied)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            )
            for row in rows:
                if row["org_id"] is None or row["month"] is None:
                    continue
                session.execute(
                    prepared,
                    (
                        row["org_id"],
                        row["month"],
                        float(row["revenue_usd"]) if row["revenue_usd"] is not None else None,
                        float(row["credits_usd"]) if row["credits_usd"] is not None else None,
                        float(row["taxes_usd"]) if row["taxes_usd"] is not None else None,
                        float(row["net_revenue_usd"]) if row["net_revenue_usd"] is not None else None,
                        float(row["fx_applied"]) if row["fx_applied"] is not None else None,
                    ),
                )
        elif table == "tickets_by_org_date":
            prepared = session.prepare(
                f"""
                INSERT INTO {table}
                (org_id, month_bucket, event_date, ticket_count, sla_breach_rate, csat_avg, severity_breakdown)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            )
            for row in rows:
                if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
                    continue
                session.execute(
                    prepared,
                    (
                        row["org_id"],
                        row["month_bucket"],
                        row["event_date"],
                        int(row["ticket_count"]) if row["ticket_count"] is not None else None,
                        float(row["sla_breach_rate"]) if row["sla_breach_rate"] is not None else None,
                        float(row["csat_avg"]) if row["csat_avg"] is not None else None,
                        {k: int(v) for k, v in row["severity_breakdown"].items()} if row["severity_breakdown"] is not None else None,
                    ),
                )
        elif table == "genai_tokens_by_org_date":
            prepared = session.prepare(
                f"""
                INSERT INTO {table}
                (org_id, month_bucket, event_date, genai_tokens_total, estimated_cost_usd)
                VALUES (?, ?, ?, ?, ?)
                """
            )
            for row in rows:
                if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
                    continue
                session.execute(
                    prepared,
                    (
                        row["org_id"],
                        row["month_bucket"],
                        row["event_date"],
                        int(row["genai_tokens_total"]) if row["genai_tokens_total"] is not None else None,
                        float(row["estimated_cost_usd"]) if row["estimated_cost_usd"] is not None else None,
                    ),
                )
        elif table == "cost_anomaly_mart":
            prepared = session.prepare(
                f"""
                INSERT INTO {table}
                (org_id, month_bucket, event_date, service, anomaly_events_count, events_count, quality_score, z_score, anomaly_zscore_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
            for row in rows:
                if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
                    continue
                session.execute(
                    prepared,
                    (
                        row["org_id"],
                        row["month_bucket"],
                        row["event_date"],
                        row["service"],
                        int(row["anomaly_events_count"]) if row["anomaly_events_count"] is not None else None,
                        int(row["events_count"]) if row["events_count"] is not None else None,
                        float(row["quality_score"]) if row["quality_score"] is not None else None,
                        float(row["z_score"]) if row["z_score"] is not None else None,
                        bool(row["anomaly_zscore_flag"]) if row["anomaly_zscore_flag"] is not None else None,
                    ),
                )
    finally:
        session.shutdown()
        cluster.shutdown()


def build_partition_writer(host: Optional[str], port: int, username: Optional[str], password: Optional[str],
                           keyspace: str, table: str,
                           astradb_bundle: Optional[str] = None, astradb_token: Optional[str] = None):
    connection = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "keyspace": keyspace,
        "table": table,
        "astradb_bundle": astradb_bundle,
        "astradb_token": astradb_token,
    }
    return partial(write_cassandra_rows, connection=connection)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.config:
        if not Path(args.config).exists():
            print(f"[ERROR] Config file not found: {args.config}")
            return 2
        args = load_config(args.config, args)
        print(f"[INFO] Loaded config from: {args.config}")

    generate_cql_files(args.cql_dir, args.keyspace)
    print(f"[INFO] CQL schemas generated in {args.cql_dir}")

    spark = (
        SparkSession.builder
        .appName("gold_to_serving_cassandra")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    gold_path = args.gold_root / args.table
    if not gold_path.exists():
        print(f"[WARN] Gold path does not exist yet: {gold_path}. Waiting for data...")
        gold_path.mkdir(parents=True, exist_ok=True)

    try:
        if not args.write_serving:
            print("[INFO] Dry-run mode (no Cassandra write). Checking Gold schema...")
            # If path has data, print count
            if any(gold_path.rglob("*.parquet")):
                df = spark.read.schema(SCHEMAS[args.table]).parquet(str(gold_path))
                print(f"[INFO] Dry-run: read {df.count()} records for table {args.table}")
            return 0

        using_astra = bool(args.astradb_bundle)
        if using_astra:
            if not args.astradb_token:
                raise ValueError("--astradb-token is required when --astradb-bundle is provided.")
            if not Path(args.astradb_bundle).exists():
                raise FileNotFoundError(f"Secure connect bundle not found: {args.astradb_bundle}")
            print(f"[INFO] AstraDB mode: bundle={args.astradb_bundle}")
        else:
            if not args.host:
                raise ValueError("Either --host (local mode) or --astradb-bundle (AstraDB mode) is required with --write-serving.")
            if not is_port_open(args.host, args.port):
                print(f"[ERROR] Cannot connect to Cassandra endpoint {args.host}:{args.port}. Is Docker running?")
                return 2
            print(f"[INFO] Local mode: {args.host}:{args.port}")

        # Initialize schema for ALL tables on Driver
        try:
            initialize_cassandra_schema(
                host=args.host,
                port=args.port,
                username=args.username,
                password=args.password,
                keyspace=args.keyspace,
                astradb_bundle=args.astradb_bundle,
                astradb_token=args.astradb_token,
            )
            print("[INFO] Cassandra Keyspace & Tables schemas initialized/verified successfully on the Driver.")
        except Exception as exc:
            print(f"[ERROR] Failed to initialize Cassandra schema from Driver: {exc}")
            return 2

        # Driver mode is stable for local/TP-sized data. Executor mode parallelizes
        # Cassandra writes with foreachPartition for larger loads.
        row_writer = build_partition_writer(
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            keyspace=args.keyspace,
            table=args.table,
            astradb_bundle=args.astradb_bundle,
            astradb_token=args.astradb_token,
        )

        is_batch_table = args.table in ("revenue_by_org_month", "tickets_by_org_date")
        print(f"[INFO] Cassandra write mode: {args.write_mode}")

        if is_batch_table:
            print(f"[INFO] Table '{args.table}' is BATCH. Reading Gold parquet statically...")
            gold_df = spark.read.schema(SCHEMAS[args.table]).parquet(str(gold_path))
            gold_df.persist()
            rows = gold_df.count()
            print(f"[INFO] Processing static table '{args.table}' (rows: {rows})")
            if rows > 0:
                if args.write_mode == "executor":
                    gold_df.rdd.foreachPartition(row_writer)
                else:
                    row_writer(gold_df.toLocalIterator())
            gold_df.unpersist()
            print(f"[OK] Batch load to Cassandra table '{args.table}' completed successfully.")
        else:
            print(f"[INFO] Table '{args.table}' is STREAMING. Starting serving stream...")
            gold_stream_df = spark.readStream.schema(SCHEMAS[args.table]).parquet(str(gold_path))

            def write_micro_batch(batch_df, batch_id):
                batch_df.persist()
                rows = batch_df.count()
                print(f"[INFO] Processing micro-batch {batch_id} (rows: {rows})")
                if rows > 0:
                    if args.write_mode == "executor":
                        batch_df.rdd.foreachPartition(row_writer)
                    else:
                        row_writer(batch_df.toLocalIterator())
                batch_df.unpersist()

            mode_str = "astradb" if using_astra else "local"
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
