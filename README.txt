
Cloud Provider Analytics Challenge — Synthetic Dataset

Folders:
- datalake/landing/customers_orgs.csv
- datalake/landing/users.csv
- datalake/landing/resources.csv
- datalake/landing/support_tickets.csv
- datalake/landing/marketing_touches.csv
- datalake/landing/nps_surveys.csv
- datalake/landing/billing_monthly.csv
- datalake/landing/usage_events_stream/*.jsonl  (Structured Streaming friendly; schema version changes ~2025-07-18)

Notes:
- Dataset spans ~60 days of event data. 
- Expect NULLs, type inconsistencies (e.g., "value" sometimes as string), negative costs, and outliers.
- "genai_tokens" and "carbon_kg" only appear after 2025-07-18 (schema_version=2).
- Monthly billing covers months: 2025-06-01, 2025-07-01, 2025-08-01.
- Regions, services, and other fields are deliberately varied to require conformance and cleaning.
