CREATE KEYSPACE IF NOT EXISTS finops
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- 1) org_daily_usage_by_service: query por org + month + date + servicio
CREATE TABLE IF NOT EXISTS finops.org_daily_usage_by_service (
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
CREATE TABLE IF NOT EXISTS finops.revenue_by_org_month (
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
CREATE TABLE IF NOT EXISTS finops.tickets_by_org_date (
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
CREATE TABLE IF NOT EXISTS finops.genai_tokens_by_org_date (
    org_id text,
    month_bucket text,
    event_date date,
    genai_tokens_total bigint,
    estimated_cost_usd double,
    PRIMARY KEY ((org_id, month_bucket), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

-- 5) cost_anomaly_mart: query por org + month + event_date + service para ver anomalias (flag y zscore)
CREATE TABLE IF NOT EXISTS finops.cost_anomaly_mart (
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
