#!/usr/bin/env python3
"""
Demo de Consulta #2: Top-N servicios por costo acumulado en los últimos 14 días.

Reusa la misma lógica de conexión que gold_to_serving_cassandra.py
(soporta AstraDB cloud o Cassandra local via cassandra_config.json).

Uso:
    python query2_top_n_demo.py --config cassandra_config.json --org-id org_xaji0y6d --top-n 5
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict


def build_cluster(cfg: dict):
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    mode = cfg.get("mode", "local")
    section = cfg[mode]

    if mode == "astradb":
        auth_provider = PlainTextAuthProvider("token", section["token"])
        return Cluster(
            cloud={"secure_connect_bundle": section["bundle"]},
            auth_provider=auth_provider,
        ), section["keyspace"], section["table"]
    else:
        return Cluster([section["host"]], port=section.get("port", 9042)), section["keyspace"], section["table"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="cassandra_config.json")
    parser.add_argument("--org-id", type=str, default="org_xaji0y6d")
    parser.add_argument("--month-buckets", type=str, default="2025-07,2025-08")
    parser.add_argument("--date-from", type=str, default="2025-07-18")
    parser.add_argument("--date-to", type=str, default="2025-07-31")
    parser.add_argument("--top-n", type=int, default=5)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    cluster, keyspace, table = build_cluster(cfg)
    session = cluster.connect(keyspace)

    buckets = ", ".join(f"'{b.strip()}'" for b in args.month_buckets.split(","))
    query = f"""
        SELECT service, daily_cost_usd
        FROM {table}
        WHERE org_id = %s
          AND month_bucket IN ({buckets})
          AND event_date >= %s AND event_date <= %s
    """

    print(f"[INFO] Ejecutando consulta CQL para org_id={args.org_id} "
          f"rango={args.date_from}..{args.date_to} buckets=({buckets})")

    totals = defaultdict(float)
    row_count = 0
    for row in session.execute(query, (args.org_id, args.date_from, args.date_to)):
        totals[row.service] += row.daily_cost_usd
        row_count += 1

    top_n = sorted(totals.items(), key=lambda x: -x[1])[: args.top_n]

    print(f"[INFO] Filas crudas leídas de Cassandra: {row_count}")
    print(f"[OK] Top-{args.top_n} servicios por costo acumulado:")
    for rank, (service, cost) in enumerate(top_n, start=1):
        print(f"  {rank}. {service:<15} ${cost:,.4f}")

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
