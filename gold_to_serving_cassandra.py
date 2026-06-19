#!/usr/bin/env python3
"""
Gold -> Serving (Cassandra/AstraDB) (point 5).

This script:
- Reads Gold mart org_daily_usage_by_service using Spark Structured Streaming (readStream).
- Generates CQL DDL and sample queries (query-first).
- Runs schema initialization (DDL) ONCE on the Driver before starting the stream.
- Writes micro-batches to Cassandra using foreachBatch + distributed foreachPartition.

========================================================================================
ARCHITECTURAL CONSIDERATIONS & DESIGN ASSUMPTIONS:
1. STREAM PATTERN (foreachBatch + foreachPartition):
   - foreachBatch: se ejecuta una vez por micro-batch en el Driver (permite coordinar logs,
     persistencias y checkpoints).
   - foreachPartition: se ejecuta dentro de cada micro-batch, en paralelo, directamente
     en los Spark Executors. Esto evita traer todas las filas al Driver (.collect() u OOMs)
     y permite escalar la escritura a Cassandra distribuidamente.
2. FILE STREAM APPEND-ONLY CONSTRAINT:
   - Este script lee Gold mediante `readStream.parquet(...)`. Esto asume que el directorio
     Gold recibe archivos nuevos en modo APPEND (incremental).
   - Si el proceso anterior de Gold se reconstruye con reescritura total (overwrite batch),
     la lectura streaming de archivos puede comportarse de manera frágil o redundante,
     ya que Structured Streaming sobre archivos está diseñado para detectar nuevos archivos
     adicionados, no reescrituras de directorios completos.
     * Si Gold se escribe por Overwrite: Se recomienda ejecutar la carga a Cassandra en modo
       Batch tradicional, o bien correr este job como una sincronización limpia post-batch.
3. IDEMPOTENCIA EN CASSANDRA:
   - La clave primaria de la tabla está definida como:
     PRIMARY KEY ((org_id, month_bucket), event_date, service)
   - Esto actúa como un UPSERT natural en Cassandra. Si un micro-lote o partición se
     reprocesa debido a fallos de red, reintentos o restauración de checkpoints de Spark,
     Cassandra actualizará las filas existentes para las mismas claves de negocio en lugar
     de duplicar los registros físicos, garantizando consistencia e idempotencia absoluta.
========================================================================================
"""

from __future__ import annotations

import argparse
import socket
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# For streaming Parquet, Spark requires an explicit schema
GOLD_SCHEMA = T.StructType(
    [
        T.StructField("event_date", T.DateType(), True),
        T.StructField("org_id", T.StringType(), True),
        T.StructField("service", T.StringType(), True),
        T.StructField("daily_cost_usd", T.DoubleType(), True),
        T.StructField("requests", T.LongType(), True),
        T.StructField("genai_tokens_total", T.LongType(), True),
        T.StructField("carbon_kg_total", T.DoubleType(), True),
        T.StructField("events_count", T.LongType(), True),
        T.StructField("anomaly_events_count", T.LongType(), True),
        T.StructField("month_bucket", T.StringType(), True),
        T.StructField("quality_score", T.DoubleType(), True),
    ]
)


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
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=Path("datalake") / "checkpoints",
        help="Checkpoint root for streaming serving query.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep streaming query running continuously. Default runs trigger once and exits.",
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
-- Cassandra queries are highly optimized because we partition by (org_id, month_bucket)
SELECT *
FROM {keyspace}.{table}
WHERE org_id = 'org_001' AND month_bucket = '2025-08';

-- Query #2: Specific day + service lookup (drill-down using clustering keys)
SELECT *
FROM {keyspace}.{table}
WHERE org_id = 'org_001' AND month_bucket = '2025-08' AND event_date = '2025-08-15' AND service = 'compute';
""".strip() + "\n"

    (cql_dir / "01_schema_finops.cql").write_text(ddl, encoding="utf-8")
    (cql_dir / "02_queries_finops.cql").write_text(queries, encoding="utf-8")


def is_port_open(host: str, port: int, timeout_sec: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except OSError:
        return False


def initialize_cassandra_schema(host: str, port: int, username: Optional[str], password: Optional[str], keyspace: str, table: str) -> None:
    """
    Initializes/verifies Keyspace and Table schema exactly ONCE on the Spark Driver
    prior to starting the Structured Streaming process.
    This avoids redundant DDL statements being executed on individual executors or per partition.
    """
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    auth_provider = None
    if username and password:
        auth_provider = PlainTextAuthProvider(username=username, password=password)

    cluster = Cluster([host], port=port, auth_provider=auth_provider)
    session = cluster.connect()

    print(f"[INFO] Initializing schema '{keyspace}.{table}' on Cassandra endpoint...")
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

    session.shutdown()
    cluster.shutdown()


def build_partition_writer(host: str, port: int, username: Optional[str], password: Optional[str], keyspace: str, table: str):
    """
    Creates a serialized closure function that will run on Spark Executors
    to write a single partition of a micro-batch into Cassandra.
    
    Optimizations:
    - Connects directly to the keyspace ('cluster.connect(keyspace)'), simplifying queries.
    - Excludes DDL initialization logic entirely to keep execution focused only on write E/S.
    """
    def write_partition(rows):
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)

        cluster = Cluster([host], port=port, auth_provider=auth_provider)
        # Connect directly to the specific keyspace
        session = cluster.connect(keyspace)

        prepared = session.prepare(
            f"""
            INSERT INTO {table}
            (org_id, month_bucket, event_date, service, daily_cost_usd, requests,
             genai_tokens_total, carbon_kg_total, events_count, anomaly_events_count, quality_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        for row in rows:
            # Skip invalid rows lacking primary partition/clustering keys
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
                    int(row["genai_tokens_total"]) if row["genai_tokens_total"] is not None else None,
                    float(row["carbon_kg_total"]) if row["carbon_kg_total"] is not None else None,
                    int(row["events_count"]) if row["events_count"] is not None else None,
                    int(row["anomaly_events_count"]) if row["anomaly_events_count"] is not None else None,
                    float(row["quality_score"]) if row["quality_score"] is not None else None,
                ),
            )

        session.shutdown()
        cluster.shutdown()

    return write_partition


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    generate_cql_files(args.cql_dir, args.keyspace, args.table)
    print(f"[INFO] CQL generated: {args.cql_dir / '01_schema_finops.cql'}")
    print(f"[INFO] CQL generated: {args.cql_dir / '02_queries_finops.cql'}")

    # Build Spark supporting dynamic schema inference in streaming
    spark = (
        SparkSession.builder
        .appName("gold_to_serving_cassandra_streaming")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    gold_path = args.gold_root / "org_daily_usage_by_service"
    if not gold_path.exists():
        print(f"[WARN] Gold path does not exist yet: {gold_path}. Waiting for data...")
        gold_path.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Read Gold parquet directory as a STREAM
        gold_stream_df = (
            spark.readStream
            .schema(GOLD_SCHEMA)
            .parquet(str(gold_path))
        )

        if not args.write_serving:
            print("[INFO] Dry-run mode (no Cassandra write). Streaming query dry-run.")
            print(f"[INFO] Gold stream schema: {gold_stream_df.schema}")
            return 0

        if not args.host:
            raise ValueError("--host is required when --write-serving is enabled.")

        if not is_port_open(args.host, args.port):
            print(f"[ERROR] Cannot connect to Cassandra endpoint {args.host}:{args.port}.")
            return 2

        # 2. Run schema initialization ONCE on Driver to ensure keyspace and table exist
        try:
            initialize_cassandra_schema(
                host=args.host,
                port=args.port,
                username=args.username,
                password=args.password,
                keyspace=args.keyspace,
                table=args.table,
            )
            print("[INFO] Cassandra Keyspace & Table schemas initialized/verified successfully on the Driver.")
        except Exception as exc:
            print(f"[ERROR] Failed to initialize Cassandra schema from Driver: {exc}")
            return 2

        # 3. Create executor partition writer function
        partition_writer = build_partition_writer(
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            keyspace=args.keyspace,
            table=args.table,
        )

        # 4. Define micro-batch handler callback
        def write_micro_batch(batch_df, batch_id):
            """
            This callback executes on the Driver for every micro-batch.
            Using batch_df.persist() prevents the micro-batch from being computed twice
            (once for the .count() action and once for the .foreachPartition() action).
            """
            # Persist the micro-batch to optimize performance and prevent re-evaluation
            batch_df.persist()
            rows = batch_df.count()
            print(f"[INFO] Processing micro-batch {batch_id} (rows: {rows})")

            if rows > 0:
                # Distribute the write using Spark's foreachPartition.
                # foreachPartition runs in parallel on executors per partition of the micro-batch,
                # avoiding bringing rows to the Driver (No collect/OOMs) and ensuring scalability.
                batch_df.rdd.foreachPartition(partition_writer)

            batch_df.unpersist()

        # 5. Configure the write stream with foreachBatch
        checkpoint_dir = args.checkpoint_root / "gold_to_serving" / args.table
        trigger_builder = {"once": True} if not args.continuous else {"processingTime": "30 seconds"}

        print(f"[INFO] Starting serving stream write to Cassandra Keyspace: '{args.keyspace}' Table: '{args.table}'")
        query = (
            gold_stream_df.writeStream
            .foreachBatch(write_micro_batch)
            .option("checkpointLocation", str(checkpoint_dir))
            .trigger(**trigger_builder)
            .start()
        )

        query.awaitTermination()
        print("[OK] Serving stream terminated successfully.")
        return 0

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
