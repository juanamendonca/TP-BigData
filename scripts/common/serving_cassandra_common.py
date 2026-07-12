#!/usr/bin/env python3
"""Shared Cassandra serving helpers for Gold marts."""

from __future__ import annotations

import argparse
import json
import socket
from functools import partial
from pathlib import Path
from typing import Callable, Iterable, Optional

from pyspark.sql import SparkSession
from pyspark.sql import types as T


BATCH_TABLES = ("revenue_by_org_month", "tickets_by_org_date")
STREAMING_TABLES = (
    "org_daily_usage_by_service",
    "cost_anomaly_mart",
    "genai_tokens_by_org_date",
)


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


def add_common_arguments(parser: argparse.ArgumentParser, table_choices: Iterable[str]) -> None:
    table_choices = tuple(table_choices)
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
        choices=list(table_choices),
        default=table_choices[0],
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
        help=(
            "Path to cassandra_config.json (see cassandra_config.example.json). "
            "Overrides defaults; explicit CLI args override config file."
        ),
    )


def load_config(config_path: str, args: argparse.Namespace) -> argparse.Namespace:
    with open(config_path, "r", encoding="utf-8") as f:
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

-- Consulta #2: Datos para Top-N servicios por costo acumulado en los ultimos 14 dias para una organizacion.
SELECT service, daily_cost_usd
FROM {keyspace}.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d'
  AND month_bucket IN ('2025-07', '2025-08')
  AND event_date >= '2025-07-18' AND event_date <= '2025-07-31';

-- Consulta #3: Evolucion de tickets criticos y tasa de SLA breach por dia (ultimos 30 dias).
-- Permite visualizar la evolucion de tickets de soporte y SLA breaches.
-- El desglose por severidad se extrae de la coleccion severity_breakdown (map).
SELECT event_date, ticket_count, sla_breach_rate, csat_avg, severity_breakdown
FROM {keyspace}.tickets_by_org_date
WHERE org_id = 'org_xaji0y6d'
  AND month_bucket = '2025-07'
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #4: Revenue mensual con creditos/impuestos aplicados (normalizado a USD).
-- Muestra la facturacion neta mensual consolidada de la organizacion.
SELECT org_id, month, revenue_usd, credits_usd, taxes_usd, net_revenue_usd, fx_applied
FROM {keyspace}.revenue_by_org_month
WHERE org_id = 'org_xaji0y6d';

-- Consulta #5: Tokens GenAI y costo estimado por dia.
-- Permite monitorear el consumo de Inteligencia Artificial Generativa y sus costos asociados por organizacion.
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


def build_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def _build_cluster(host, port, username, password, astradb_bundle=None, astradb_token=None):
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    if astradb_bundle:
        auth_provider = PlainTextAuthProvider("token", astradb_token)
        return Cluster(cloud={"secure_connect_bundle": astradb_bundle}, auth_provider=auth_provider)

    auth_provider = None
    if username and password:
        auth_provider = PlainTextAuthProvider(username=username, password=password)
    return Cluster([host], port=port, auth_provider=auth_provider)


def initialize_cassandra_schema(
    host: Optional[str],
    port: int,
    username: Optional[str],
    password: Optional[str],
    keyspace: str,
    astradb_bundle: Optional[str] = None,
    astradb_token: Optional[str] = None,
) -> None:
    cluster = _build_cluster(host, port, username, password, astradb_bundle, astradb_token)
    session = cluster.connect()

    print("[INFO] Initializing keyspace and tables on Cassandra...")

    if not astradb_bundle:
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )

    session.set_keyspace(keyspace)

    session.execute(
        """
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
    session.execute(
        """
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
    session.execute(
        """
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
    session.execute(
        """
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
    session.execute(
        """
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


def _float_or_none(value):
    return float(value) if value is not None else None


def _int_or_none(value):
    return int(value) if value is not None else None


def _bool_or_none(value):
    return bool(value) if value is not None else None


def transform_org_daily_usage_by_service(row):
    if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
        return None
    return (
        row["org_id"],
        row["month_bucket"],
        row["event_date"],
        row["service"],
        _float_or_none(row["daily_cost_usd"]),
        _int_or_none(row["requests"]),
        _float_or_none(row["cpu_hours"]),
        _float_or_none(row["storage_gb_hours"]),
        _int_or_none(row["genai_tokens_total"]),
        _float_or_none(row["carbon_kg_total"]),
        _int_or_none(row["events_count"]),
        _int_or_none(row["anomaly_events_count"]),
        _float_or_none(row["quality_score"]),
    )


def transform_revenue_by_org_month(row):
    if row["org_id"] is None or row["month"] is None:
        return None
    return (
        row["org_id"],
        row["month"],
        _float_or_none(row["revenue_usd"]),
        _float_or_none(row["credits_usd"]),
        _float_or_none(row["taxes_usd"]),
        _float_or_none(row["net_revenue_usd"]),
        _float_or_none(row["fx_applied"]),
    )


def transform_tickets_by_org_date(row):
    if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
        return None
    severity_breakdown = row["severity_breakdown"]
    return (
        row["org_id"],
        row["month_bucket"],
        row["event_date"],
        _int_or_none(row["ticket_count"]),
        _float_or_none(row["sla_breach_rate"]),
        _float_or_none(row["csat_avg"]),
        {k: int(v) for k, v in severity_breakdown.items()} if severity_breakdown is not None else None,
    )


def transform_genai_tokens_by_org_date(row):
    if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
        return None
    return (
        row["org_id"],
        row["month_bucket"],
        row["event_date"],
        _int_or_none(row["genai_tokens_total"]),
        _float_or_none(row["estimated_cost_usd"]),
    )


def transform_cost_anomaly_mart(row):
    if row["org_id"] is None or row["month_bucket"] is None or row["event_date"] is None:
        return None
    return (
        row["org_id"],
        row["month_bucket"],
        row["event_date"],
        row["service"],
        _int_or_none(row["anomaly_events_count"]),
        _int_or_none(row["events_count"]),
        _float_or_none(row["quality_score"]),
        _float_or_none(row["z_score"]),
        _bool_or_none(row["anomaly_zscore_flag"]),
    )


TABLE_INSERTS: dict[str, tuple[str, Callable]] = {
    "org_daily_usage_by_service": (
        """
        INSERT INTO org_daily_usage_by_service
        (org_id, month_bucket, event_date, service, daily_cost_usd, requests,
         cpu_hours, storage_gb_hours, genai_tokens_total, carbon_kg_total,
         events_count, anomaly_events_count, quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        transform_org_daily_usage_by_service,
    ),
    "revenue_by_org_month": (
        """
        INSERT INTO revenue_by_org_month
        (org_id, month, revenue_usd, credits_usd, taxes_usd, net_revenue_usd, fx_applied)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        transform_revenue_by_org_month,
    ),
    "tickets_by_org_date": (
        """
        INSERT INTO tickets_by_org_date
        (org_id, month_bucket, event_date, ticket_count, sla_breach_rate, csat_avg, severity_breakdown)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        transform_tickets_by_org_date,
    ),
    "genai_tokens_by_org_date": (
        """
        INSERT INTO genai_tokens_by_org_date
        (org_id, month_bucket, event_date, genai_tokens_total, estimated_cost_usd)
        VALUES (?, ?, ?, ?, ?)
        """,
        transform_genai_tokens_by_org_date,
    ),
    "cost_anomaly_mart": (
        """
        INSERT INTO cost_anomaly_mart
        (org_id, month_bucket, event_date, service, anomaly_events_count, events_count,
         quality_score, z_score, anomaly_zscore_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        transform_cost_anomaly_mart,
    ),
}


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

    try:
        statement, transform_row = TABLE_INSERTS[connection["table"]]
        prepared = session.prepare(statement)
        for row in rows:
            values = transform_row(row)
            if values is not None:
                session.execute(prepared, values)
    finally:
        session.shutdown()
        cluster.shutdown()


def build_partition_writer(
    host: Optional[str],
    port: int,
    username: Optional[str],
    password: Optional[str],
    keyspace: str,
    table: str,
    astradb_bundle: Optional[str] = None,
    astradb_token: Optional[str] = None,
):
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


def prepare_args(args: argparse.Namespace) -> argparse.Namespace | int:
    if args.config:
        if not Path(args.config).exists():
            print(f"[ERROR] Config file not found: {args.config}")
            return 2
        args = load_config(args.config, args)
        print(f"[INFO] Loaded config from: {args.config}")

    generate_cql_files(args.cql_dir, args.keyspace)
    print(f"[INFO] CQL schemas generated in {args.cql_dir}")
    return args


def ensure_gold_path(gold_root: Path, table: str) -> Path:
    gold_path = gold_root / table
    if not gold_path.exists():
        print(f"[WARN] Gold path does not exist yet: {gold_path}. Waiting for data...")
        gold_path.mkdir(parents=True, exist_ok=True)
    return gold_path


def validate_cassandra_target(args: argparse.Namespace) -> bool:
    using_astra = bool(args.astradb_bundle)
    if using_astra:
        if not args.astradb_token:
            raise ValueError("--astradb-token is required when --astradb-bundle is provided.")
        if not Path(args.astradb_bundle).exists():
            raise FileNotFoundError(f"Secure connect bundle not found: {args.astradb_bundle}")
        print(f"[INFO] AstraDB mode: bundle={args.astradb_bundle}")
        return True

    if not args.host:
        raise ValueError("Either --host (local mode) or --astradb-bundle (AstraDB mode) is required with --write-serving.")
    if not is_port_open(args.host, args.port):
        print(f"[ERROR] Cannot connect to Cassandra endpoint {args.host}:{args.port}. Is Docker running?")
        return False
    print(f"[INFO] Local mode: {args.host}:{args.port}")
    return True


def initialize_serving(args: argparse.Namespace):
    if not validate_cassandra_target(args):
        return None

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
        return None

    return build_partition_writer(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        keyspace=args.keyspace,
        table=args.table,
        astradb_bundle=args.astradb_bundle,
        astradb_token=args.astradb_token,
    )


def write_dataframe_to_cassandra(df, row_writer, write_mode: str, label: str) -> int:
    df.persist()
    try:
        rows = df.count()
        print(f"[INFO] Processing {label} (rows: {rows})")
        if rows > 0:
            if write_mode == "executor":
                df.rdd.foreachPartition(row_writer)
            else:
                row_writer(df.toLocalIterator())
        return rows
    finally:
        df.unpersist()
