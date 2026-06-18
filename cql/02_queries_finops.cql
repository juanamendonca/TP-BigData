-- Query #1: Daily usage/cost by service for one org in a month bucket
SELECT *
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_001' AND month_bucket = '2025-08';

-- Query #2: Specific day + service lookup (drill-down)
SELECT *
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_001' AND month_bucket = '2025-08' AND event_date = '2025-08-15' AND service = 'compute';
