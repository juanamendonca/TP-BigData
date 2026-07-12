-- Consulta #1: Costos y requests diarios por org y servicio en un rango de fechas.
SELECT org_id, month_bucket, event_date, service, daily_cost_usd, requests
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d'
  AND month_bucket = '2025-07'
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #2: Datos para Top-N servicios por costo acumulado en los ultimos 14 dias para una organizacion.
SELECT service, daily_cost_usd
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d'
  AND month_bucket IN ('2025-07', '2025-08')
  AND event_date >= '2025-07-18' AND event_date <= '2025-07-31';

-- Consulta #3: Evolucion de tickets criticos y tasa de SLA breach por dia (ultimos 30 dias).
-- Permite visualizar la evolucion de tickets de soporte y SLA breaches.
-- El desglose por severidad se extrae de la coleccion severity_breakdown (map).
SELECT event_date, ticket_count, sla_breach_rate, csat_avg, severity_breakdown
FROM finops.tickets_by_org_date
WHERE org_id = 'org_xaji0y6d'
  AND month_bucket = '2025-07'
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #4: Revenue mensual con creditos/impuestos aplicados (normalizado a USD).
-- Muestra la facturacion neta mensual consolidada de la organizacion.
SELECT org_id, month, revenue_usd, credits_usd, taxes_usd, net_revenue_usd, fx_applied
FROM finops.revenue_by_org_month
WHERE org_id = 'org_xaji0y6d';

-- Consulta #5: Tokens GenAI y costo estimado por dia.
-- Permite monitorear el consumo de Inteligencia Artificial Generativa y sus costos asociados por organizacion.
SELECT event_date, genai_tokens_total, estimated_cost_usd
FROM finops.genai_tokens_by_org_date
WHERE org_id = 'org_xaji0y6d'
  AND month_bucket = '2025-07'
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';
