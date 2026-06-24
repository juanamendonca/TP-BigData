-- Consulta #1: Costos y requests diarios por org y servicio en un rango de fechas.
SELECT org_id, month_bucket, event_date, service, daily_cost_usd, requests
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket = '2025-07' 
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #2: Datos para Top-N servicios por costo acumulado en los últimos 14 días para una organización.
SELECT service, daily_cost_usd
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket IN ('2025-07', '2025-08') 
  AND event_date >= '2025-07-18' AND event_date <= '2025-07-31';

-- Consulta #3: Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
-- Permite visualizar la evolución de tickets de soporte y SLA breaches. 
-- El desglose por severidad se extrae de la colección severity_breakdown (map).
SELECT event_date, ticket_count, sla_breach_rate, csat_avg, severity_breakdown
FROM finops.tickets_by_org_date
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket = '2025-07' 
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD).
-- Muestra la facturación neta mensual consolidada de la organización.
SELECT org_id, month, revenue_usd, credits_usd, taxes_usd, net_revenue_usd, fx_applied
FROM finops.revenue_by_org_month
WHERE org_id = 'org_xaji0y6d';

-- Consulta #5: Tokens GenAI y costo estimado por día.
-- Permite monitorear el consumo de Inteligencia Artificial Generativa y sus costos asociados por organización.
SELECT event_date, genai_tokens_total, estimated_cost_usd
FROM finops.genai_tokens_by_org_date
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket = '2025-07' 
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';
