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
- Escribe Bronze en formato Parquet particionado por batch_date.
- Genera manifest de control con conteos de lectura, post-dedupe y escritura.
- Es idempotente por particion: reescribe la particion del batch_date ejecutado.

Tablas soportadas:
- billing_monthly
- customers_orgs
- marketing_touches
- nps_surveys
- resources
- support_tickets
- users

## Como correr

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
- datalake/bronze/batch/<tabla>/batch_date=YYYY-MM-DD/

Manifest de corrida:
- datalake/bronze/_control/batch_date=YYYY-MM-DD/manifest.json

## Evidencia de cumplimiento (Batch -> Bronze)

- Formato Bronze: Parquet particionado.
- Tipificacion: esquema explicito por cada CSV.
- Trazabilidad: ingest_ts y source_file.
- Dedupe: dropDuplicates por clave definida por tabla.
- Idempotencia: overwrite dinamico por particion batch_date.

## Parte 2: Streaming -> Bronze (PySpark Structured Streaming)

Script principal:
- streaming_landing_to_bronze.py

### Que hace el script

- Lee usage_events_stream/*.jsonl con Structured Streaming.
- Usa esquema explicito unificado para schema_version 1 y 2.
- Aplica withWatermark sobre event_ts.
- Aplica dedupe por event_id.
- Maneja late data enviandola a quarantine.
- Habilita checkpointing para tolerancia a reinicios.
- Agrega columnas tecnicas:
  - ingest_ts
  - source_file
  - batch_date
- Escribe Parquet particionado por event_date.

### Como correr Parte 2

Modo recomendado para entrega (trigger once):

```powershell
python streaming_landing_to_bronze.py --watermark-delay "2 days" --late-threshold-hours 48 --max-files-per-trigger 50
```

Modo continuo (simulacion near real-time):

```powershell
python streaming_landing_to_bronze.py --continuous --watermark-delay "2 days" --late-threshold-hours 48
```

En Linux/macOS usar los mismos comandos en shell bash/zsh.

### Salidas esperadas Parte 2

- Bronze streaming: datalake/bronze/streaming/usage_events/event_date=YYYY-MM-DD/
- Quarantine invalidos: datalake/bronze/quarantine/usage_events_invalid/batch_date=YYYY-MM-DD/
- Quarantine late: datalake/bronze/quarantine/usage_events_late/event_date=YYYY-MM-DD/
- Checkpoints: datalake/checkpoints/streaming_landing_to_bronze/

### Nota de entorno (importante)

Si aparece el error "getSubject is not supported" al iniciar Spark, usar Java 17 para ejecutar PySpark.
Con versiones de Java mas nuevas puede fallar Hadoop en Windows.

### Troubleshooting rapido (Windows)

Si aparece "UnsupportedOperationException: getSubject is not supported":

1) Reinstalar PySpark estable para este TP:

```powershell
python -m pip uninstall -y pyspark py4j
python -m pip install pyspark==3.5.2
```

2) Configurar Java 17 para la sesion actual:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
```

3) Volver a correr:

```powershell
python batch_landing_to_bronze.py --batch-date 2026-06-15
python streaming_landing_to_bronze.py --watermark-delay "2 days" --late-threshold-hours 48 --max-files-per-trigger 50
```

Si no tenes jdk-17 instalado, instalar Temurin/Oracle JDK 17 y repetir los pasos.
