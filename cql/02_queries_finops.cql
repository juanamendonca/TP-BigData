-- Consulta #1: Costos y requests diarios por org y servicio en un rango de fechas.
SELECT org_id, month_bucket, event_date, service, daily_cost_usd, requests
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket = '2025-07' 
  AND event_date >= '2025-07-01' AND event_date <= '2025-07-31';

-- Consulta #2: Datos para Top-N servicios por costo acumulado en los últimos 14 días para una organización.
-- Cassandra (OLTP) no soporta ordenamientos dinámicos por métricas (daily_cost_usd) ni sumas acumuladas en tiempo de ejecución.
-- Para resolver la consulta #2, se leen los datos del rango de fechas desde Cassandra:
SELECT service, daily_cost_usd
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' 
  AND month_bucket IN ('2025-07', '2025-08') 
  AND event_date >= '2025-07-18' AND event_date <= '2025-07-31';
-- Y luego se realiza la agregación (SUM por servicio) y ordenamiento (Top-N).
