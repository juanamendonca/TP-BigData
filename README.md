# TP-BigData - Segunda Entrega (MVP tecnico)


Objetivo de la entrega: demostrar el flujo end-to-end(Landing -> Bronze -> Silver -> Gold -> Serving).

## Instalacion global del TP

### Requisitos base

- Python 3.10+ (recomendado: usar el .venv del proyecto)
- Java 11 o 17 (requerido por Spark)

### 1) Activar entorno virtual (Windows PowerShell)

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

### 2) Instalar dependencias base

```powershell
python -m pip install --upgrade pip
python -m pip install pyspark==3.5.2
```

### 3) Verificar instalacion

```powershell
python -m pip show pyspark
java -version
```

Version recomendada para Windows en este TP:
- pyspark 3.5.2
- Java 17 (preferido) o Java 11

### Instalacion global en Linux/macOS

1) Crear y activar entorno virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2) Instalar dependencias base:

```bash
python -m pip install --upgrade pip
python -m pip install pyspark==3.5.2
```

3) Instalar Java 17:

Linux (Ubuntu/Debian):

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
```

macOS (Homebrew):

```bash
brew install openjdk@17
```

4) Exportar JAVA_HOME en la sesion actual:

Linux:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

macOS:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

5) Verificar instalacion:

```bash
python -m pip show pyspark
java -version
```

## Parte 1: Batch -> Bronze (PySpark)

Script principal:
- batch_landing_to_bronze.py

### Que hace el script

- Lee maestros CSV desde datalake/landing con esquema explicito (sin inferencia).
- Aplica deduplicacion por claves de negocio por tabla.
- Agrega columnas tecnicas:
  - ingest_ts
  - source_file
  - batch_date
- Deriva columnas temporales para particionado natural cuando aplica:
  - created_date = to_date(created_at) en support_tickets
  - touch_date = to_date(timestamp) en marketing_touches
- Escribe Bronze en formato Parquet con particionado sensato por tabla:
  - billing_monthly: month
  - support_tickets: created_date
  - marketing_touches: touch_date
  - customers_orgs y users: load_date
  - nps_surveys: survey_date
  - resources: batch_date
- Aplica controles basicos de calidad: filtro NOT NULL sobre claves criticas.
- Genera manifest de control con conteos de lectura, post-calidad, post-dedupe y escritura.
- Es idempotente por particion: reescribe las particiones objetivo del lote ejecutado.

Tablas soportadas:
- billing_monthly
- customers_orgs
- marketing_touches
- nps_surveys
- resources
- support_tickets
- users

## Como correr Parte 1

### Ejecucion (3 tablas)

```powershell
python batch_landing_to_bronze.py --batch-date 2026-06-15 --tables customers_orgs,users,billing_monthly
```

### Ejecucion completa (todos las tablas)

```powershell
python batch_landing_to_bronze.py --batch-date 2026-06-15
```

### Ejecucion en Linux/macOS

Batch (3 tablas):

```bash
python batch_landing_to_bronze.py --batch-date 2026-06-15 --tables customers_orgs,users,billing_monthly
```

Batch completo:

```bash
python batch_landing_to_bronze.py --batch-date 2026-06-15
```

### Parametros disponibles

- --landing-root: ruta de entrada (default: datalake/landing)
- --bronze-root: ruta de salida (default: datalake/bronze)
- --batch-date: fecha de particion en formato YYYY-MM-DD
- --tables: lista separada por comas con tablas a procesar (minimo 3)

## Salidas esperadas

Parquet Bronze por tabla:
- billing_monthly: datalake/bronze/batch/billing_monthly/month=YYYY-MM-DD/
- support_tickets: datalake/bronze/batch/support_tickets/created_date=YYYY-MM-DD/
- marketing_touches: datalake/bronze/batch/marketing_touches/touch_date=YYYY-MM-DD/
- customers_orgs: datalake/bronze/batch/customers_orgs/load_date=YYYY-MM-DD/
- users: datalake/bronze/batch/users/load_date=YYYY-MM-DD/
- nps_surveys: datalake/bronze/batch/nps_surveys/survey_date=YYYY-MM-DD/
- resources: datalake/bronze/batch/resources/batch_date=YYYY-MM-DD/

Manifest de corrida:
- datalake/bronze/_control/batch_date=YYYY-MM-DD/manifest.json

## Evidencia de cumplimiento (Batch -> Bronze)

- Formato Bronze: Parquet particionado.
- Particionado sensato por tabla segun patron de consulta.
- Tipificacion: esquema explicito por cada CSV.
- Trazabilidad: ingest_ts y source_file.
- Calidad minima en Bronze: filtros NOT NULL para claves de negocio.
- Dedupe: dropDuplicates por clave definida por tabla.
- Idempotencia: overwrite dinamico por la columna de particion de cada tabla.

## Parte 2: Streaming -> Bronze (PySpark Structured Streaming)

Script principal:
- streaming_landing_to_bronze.py

### Que hace el script

- Lee usage_events_stream/*.jsonl con Structured Streaming.
- Usa esquema explicito unificado para schema_version 1 y 2.
- Aplica withWatermark sobre event_ts para tolerancia de eventos tardios.
- Aplica dedupe por event_id.
- Envia a quarantine registros invalidos (corruptos, event_id/event_ts nulos y errores de casteo de value).
- Habilita checkpointing para tolerancia a reinicios.
- Agrega columnas tecnicas:
  - ingest_ts
  - source_file
  - batch_date
- Escribe Parquet particionado por event_date.

### Como correr Parte 2

Modo recomendado para entrega (trigger once):

```powershell
python streaming_landing_to_bronze.py --watermark-delay "2 days" --max-files-per-trigger 50
```

Modo continuo (simulacion near real-time):

```powershell
python streaming_landing_to_bronze.py --continuous --watermark-delay "2 days"
```

En Linux/macOS usar los mismos comandos en shell bash/zsh.

### Salidas esperadas Parte 2

- Bronze streaming: datalake/bronze/streaming/usage_events/event_date=YYYY-MM-DD/
- Quarantine invalidos: datalake/bronze/quarantine/usage_events_invalid/batch_date=YYYY-MM-DD/
- Checkpoints: datalake/checkpoints/streaming_landing_to_bronze/

## Parte 3: Bronze -> Silver (PySpark)

Script principal:
- bronze_to_silver.py

### Que hace el script

- Lee eventos desde Bronze streaming y 1 maestro desde Bronze batch (`customers_orgs`).
- Aplica limpieza/conformance de tipos y campos (event_ts, value_num, metric, unit, costos).
- Aplica join de enriquecimiento por `org_id` con datos de organizacion.
- Activa reglas de calidad:
  - `event_id` no nulo.
  - `event_id` unico.
  - `cost_usd_increment >= -0.01` (se mantiene en Silver con `anomaly_cost_flag`).
  - `unit` no nulo cuando `value` existe.
- Envia registros con fallas duras a quarantine y guarda muestras.
- Genera features diarias por `event_date`, `org_id`, `service`:
  - `daily_cost_usd`
  - `requests`
  - `genai_tokens_total`
  - `carbon_kg_total`

### Como correr Parte 3

Windows PowerShell:

```powershell
python bronze_to_silver.py
```

Linux/macOS:

```bash
python bronze_to_silver.py
```

### Salidas esperadas Parte 3

- Silver enriquecido: `datalake/silver/events_enriched/event_date=YYYY-MM-DD/`
- Silver features: `datalake/silver/features_org_daily/event_date=YYYY-MM-DD/`
- Quarantine: `datalake/silver/quarantine/events_quality_issues/event_date=YYYY-MM-DD/`
- Muestras de quarantine: `datalake/silver/quarantine/samples/`
- Manifest: `datalake/silver/_control/manifest.json`

## Parte 4: Silver -> Gold (PySpark)

Script principal:
- silver_to_gold.py

### Que hace el script

- Lee `datalake/silver/features_org_daily`.
- Construye el mart FinOps `org_daily_usage_by_service` (grano diario por org/servicio).
- Calcula y publica metricas/costos de negocio para serving:
  - `daily_cost_usd`
  - `requests`
  - `genai_tokens_total`
  - `carbon_kg_total`
  - `events_count`
  - `anomaly_events_count`
  - `quality_score`
- Agrega `month_bucket` para modelado query-first en Cassandra.

### Como correr Parte 4

Windows PowerShell:

```powershell
python silver_to_gold.py
```

Linux/macOS:

```bash
python silver_to_gold.py
```

### Salidas esperadas Parte 4

- Gold mart: `datalake/gold/org_daily_usage_by_service/event_date=YYYY-MM-DD/`
- Manifest: `datalake/gold/_control/manifest.json`

## Parte 5: Gold -> Serving (Cassandra en Docker)

Script principal:
- gold_to_serving_cassandra.py

Archivos CQL:
- `cql/01_schema_finops.cql`
- `cql/02_queries_finops.cql`

### Que hace el script

- Lee el mart Gold `org_daily_usage_by_service`.
- Genera CQL de schema y 2 consultas minimas (query-first).
- En modo dry-run valida conteos y deja artefactos CQL.
- En modo `--write-serving` inserta datos en Cassandra usando `cassandra-driver`.

### Dependencia adicional para Parte 5

```powershell
python -m pip install cassandra-driver
```

Linux/macOS:

```bash
python -m pip install cassandra-driver
```

### Setup Cassandra local con Docker

```bash
docker run -d --name cassandra-local -p 9042:9042 cassandra:4.1
```

Esperar ~30 segundos hasta que el nodo este listo, luego verificar:

```bash
docker exec -it cassandra-local cqlsh
```

Cargar schema (dentro de cqlsh):

```sql
SOURCE '/path/to/cql/01_schema_finops.cql';
```

O copiando el contenido de `cql/01_schema_finops.cql` directamente.

### Como correr Parte 5

Generar CQL + validar datos Gold (sin cargar):

```powershell
python gold_to_serving_cassandra.py
```

Carga real a Cassandra (ejemplo local):

```powershell
python gold_to_serving_cassandra.py --write-serving --host 127.0.0.1 --port 9042 --keyspace finops --table org_daily_usage_by_service
```

Verificar conectividad antes de cargar:

Linux/macOS/WSL:

```bash
nc -zv 127.0.0.1 9042
```

Windows PowerShell:

```powershell
Test-NetConnection -ComputerName 127.0.0.1 -Port 9042
```

### Salidas esperadas Parte 5

- CQL schema: `cql/01_schema_finops.cql`
- CQL queries: `cql/02_queries_finops.cql`
- Tabla poblada: `finops.org_daily_usage_by_service`

## Evidencias de aceptacion

### 1) Batch y Streaming ejecutan con datos provistos

- Batch a Bronze ejecutado correctamente con conteos y manifest en `datalake/bronze/_control/batch_date=.../manifest.json`.
- Streaming a Bronze ejecutado correctamente en modo trigger once, con salida en:
  - `datalake/bronze/streaming/usage_events/`
  - `datalake/bronze/quarantine/usage_events_invalid/`
  - Watermark activo para tolerancia de eventos tardios.

### 2) Reglas de calidad y quarantine efectivas

- Silver aplica reglas de calidad activas (event_id, costo minimo, unit when value).
- Manifest Silver generado en `datalake/silver/_control/manifest.json`.
- Se valida que el pipeline completa y publica datasets Silver:
  - `datalake/silver/events_enriched/`
  - `datalake/silver/features_org_daily/`
  - `datalake/silver/quarantine/events_quality_issues/`

### 3) Mart FinOps en Gold

- Mart `org_daily_usage_by_service` generado en `datalake/gold/org_daily_usage_by_service/`.
- Manifest Gold en `datalake/gold/_control/manifest.json`.

### 4) Serving en Cassandra poblado

- Carga a Cassandra realizada con resultado exitoso:
  - `Rows written to Cassandra: 11050`
- Tabla de serving: `finops.org_daily_usage_by_service`.

### 5) Consultas minimas sobre Cassandra

- Query #1 (particion completa por org + month_bucket) devuelve multiples filas:

```sql
SELECT * FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' AND month_bucket = '2025-07';
```

Resultado observado: `38 rows`.

- Query #2 (drill-down por org + mes + fecha + servicio) devuelve una fila puntual:

```sql
SELECT * FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_xaji0y6d' AND month_bucket = '2025-07'
AND event_date = '2025-07-31' AND service = 'compute';
```

Resultado observado: `1 row`.

### 6) Idempotencia verificada con [VERIFY]

- Al final de cada script (Parte 1, 2 y 3) se imprime `[VERIFY] <dataset> total_rows=<n>`.
- Re-ejecutar con misma entrada y misma configuracion produce los mismos valores.
- Bronze y Silver/Gold escriben en modo `overwrite` particionado.
- Serving usa clave primaria por `((org_id, month_bucket), event_date, service)`, evitando duplicados logicos.

### 7) Diagnostico de diferencia Bronze vs Silver

- Silver imprime `Silver events before enrichment` y `Distinct org_id without customers match`.
- Permite explicar cualquier diferencia entre registros Bronze (43200) y Silver events (41162) sin ambiguedad.

## Log de decisiones

1) Patron de arquitectura
- Se mantiene Lambda segun 1er parcial: flujo batch para maestros y flujo streaming para eventos.

2) Formatos por zona
- Bronze/Silver/Gold en Parquet particionado para eficiencia y trazabilidad.

3) Criterio de particionado
- Bronze batch (por tabla):
  - billing_monthly: month
  - customers_orgs: load_date
  - users: load_date
  - marketing_touches: touch_date
  - support_tickets: created_date
  - nps_surveys: survey_date
  - resources: batch_date
- Bronze streaming: `event_date`.
- Silver y Gold: `event_date`.

4) Calidad de datos
- En Silver se aplican reglas hard-fail a quarantine y reglas soft-fail con flag de anomalia.

5) Modelo de serving (query-first)
- Cassandra modelado para consultas por organizacion y mes (`org_id`, `month_bucket`) y drill-down por fecha/servicio.

6) Decisiones de compatibilidad
- PySpark fijado en version estable para entorno local.
- Documentacion de ejecucion para Windows, Linux/macOS y Docker.
