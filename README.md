# TP-BigData - Segunda Entrega (MVP técnico)

Objetivo de la entrega: demostrar el flujo end-to-end (Landing -> Bronze -> Silver -> Gold -> Serving).

## Instalación y Configuración

### Requisitos base
* **Python 3.11.9** (Requerido: versiones 3.12+ presentan incompatibilidades con `cassandra-driver`).
* **Java 11 o 17** (Requerido por Apache Spark).

---

### Paso 1 — Crear y Activar Entorno Virtual (.venv)

Ejecutá según tu sistema operativo en la carpeta raíz del proyecto:

* **Windows (PowerShell):**
  ```powershell
  # 1. Crear entorno con Python 3.11
  py -3.11 -m venv .venv

  # 2. Activar entorno (ejecutar antes 'Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned' si da error)
  .\.venv\Scripts\Activate.ps1
  ```
* **macOS / Linux:**
  ```bash
  # 1. Crear entorno con Python 3.11
  python3.11 -m venv .venv

  # 2. Activar entorno
  source .venv/bin/activate
  ```

---

### Paso 2 — Instalar todas las dependencias
Con el entorno virtual activado, actualizá pip e instalá todos los paquetes (`pyspark`, `cassandra-driver` y `gevent` se instalan juntos aquí):
```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

---

### Paso 3 — Instalar Java 17 y Configurar Variables de Entorno
Para que Spark pueda correr en modo local, es necesario configurar las rutas de Java y las utilidades de Hadoop (winutils):

* **Windows (PowerShell):**
  Descargá e instalá JDK 17 (Microsoft JDK, Temurin o similar) y ejecutá en tu terminal:
  ```powershell
  # Java 17 (Ajustá la versión de la carpeta según tu JDK instalado)
  $env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
  $env:Path = "$env:JAVA_HOME\bin;" + $env:Path

  # Hadoop / winutils (Requerido por Spark para Windows)
  $env:HADOOP_HOME = "$PWD\hadoop"
  $env:Path = "$env:HADOOP_HOME\bin;" + $env:Path
  ```
* **macOS / Linux:**
  - **macOS (Homebrew):**
    ```bash
    brew install openjdk@17
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    export PATH="$JAVA_HOME/bin:$PATH"
    ```
  - **Linux (Ubuntu/Debian):**
    ```bash
    sudo apt update && sudo apt install -y openjdk-17-jdk
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
    ```

---

### Paso 4 — Verificar la instalación
Corré estos comandos para asegurar que la configuración está lista:
```bash
python -m pip show pyspark
java -version
```

---

## Guía de Ejecución del Pipeline (Paso a Paso)

Asegurate de estar posicionado en la carpeta raíz del proyecto, con el entorno virtual activado y las variables de entorno configuradas antes de continuar.

---

### Paso 1 — Ingesta Batch a Bronze (Maestros)
Procesa los archivos maestros desde `datalake/landing` hacia `datalake/bronze`:
```bash
python batch_landing_to_bronze.py --batch-date 2026-06-15
```
*(Opcional: podés especificar tablas separadas por comas con `--tables customers_orgs,users`).*

---

### Paso 2 — Ingesta Streaming a Bronze (Eventos de uso)
Lee incrementalmente los eventos de uso y genera la capa Bronze con watermarks:
```bash
python streaming_landing_to_bronze.py --watermark-delay "2 days" --max-files-per-trigger 50
```

---

### Paso 3 — Conformance y Enriquecimiento a Silver
Limpia los tipos de datos, aplica reglas de calidad, maneja la cuarentena y genera features diarias:
```bash
python bronze_to_silver.py --max-files-per-trigger 1000
```

---

### Paso 4 — Construcción del Mart a Gold
Construye el mart de negocio FinOps agrupado a partir de las features agregadas:
```bash
python silver_to_gold.py --max-files-per-trigger 1000
```

---

### Paso 5 — Carga al Serving (Cassandra / AstraDB)

Este paso requiere tener una base de datos activa. Elegí una de las siguientes opciones para configurar tu entorno y correr el script:

#### Opción A — AstraDB en la Nube (Recomendada)
1. **Crear base de datos:** Registrate en [astra.datastax.com](https://astra.datastax.com) y creá una base de datos Serverless con keyspace `finops`.
2. **Descargar Secure Connect Bundle:** Entrá a tu DB en el dashboard de Astra, ve a **Connect** > **Drivers** > **Python**, descargá el archivo ZIP `secure-connect-<db-name>.zip` y pegalo en la carpeta raíz del proyecto.
3. **Generar Token:** En el panel de Astra ve a **Settings** > **Application Tokens** y generá un token con el rol **Database Administrator**. Copiá el token (formato: `AstraCS:...`).
4. **Configurar archivo:** Copiá el template de configuración:
   - *Windows:* `copy cassandra_config.example.json cassandra_config.json`
   - *macOS/Linux:* `cp cassandra_config.example.json cassandra_config.json`
   
   Editá `cassandra_config.json` con tu ZIP y token:
   ```json
   {
     "mode": "astradb",
     "astradb": {
       "bundle": "secure-connect-bigdata-tp.zip",
       "token": "AstraCS:xxxxxxx...",
       "keyspace": "finops",
       "table": "org_daily_usage_by_service"
     }
   }
   ```
5. **Cargar y verificar:** Ejecutá la carga:
   ```bash
   python gold_to_serving_cassandra.py --write-serving --config cassandra_config.json
   ```
   Verificá los resultados en la **CQL Console** de la web de Astra ejecutando las consultas en `cql/02_queries_finops.cql`.

#### Opción B — Cassandra Local con Docker
1. **Levantar contenedor:** Teniendo Docker Desktop abierto, ejecutá:
   ```bash
   docker run -d --name cassandra-local -p 9042:9042 cassandra:4.1
   ```
2. **Esperar inicio:** Esperá a que el contenedor esté listo (~30-60 segundos):
   - *Windows:* `docker logs cassandra-local 2>&1 | Select-String "Startup complete"`
   - *macOS/Linux:* `docker logs cassandra-local 2>&1 | grep "Startup complete"`
3. **Configurar archivo:** Copiá el template de configuración (`cassandra_config.json`) y cambiá el modo a local:
   ```json
   {
     "mode": "local",
     "local": {
       "host": "127.0.0.1",
       "port": 9042,
       "keyspace": "finops",
       "table": "org_daily_usage_by_service"
     }
   }
   ```
4. **Cargar y verificar:** Ejecutá la carga:
   ```bash
   python gold_to_serving_cassandra.py --write-serving --config cassandra_config.json
   ```
   Para validar localmente, conectate a cqlsh:
   ```bash
   docker exec -it cassandra-local cqlsh
   ```
   Dentro de `cqlsh`, activá el keyspace corriendo `USE finops;` y luego ejecutá las consultas reales que se encuentran en el archivo `cql/02_queries_finops.cql` para corroborar los resultados.

#### Validación automática de la Consulta #2 (Top-N acumulado)
Para ver los resultados agrupados y ordenados de la consulta analítica #2 (Top-N servicios por costo acumulado en los últimos 14 días, tanto local como en AstraDB según tu configuración), ejecutá:
```bash
python query2_top_n_demo.py --config cassandra_config.json
```

---

## 📖 Arquitectura y Detalles del Diseño

Para leer en detalle el comportamiento de cada script, las salidas esperadas en disco, la validación de las reglas de calidad y el log con las decisiones de diseño arquitectónico de cada capa, consultá la documentación técnica:

👉 **[DETALLES.md](file:///c:/Users/solro/TP-BigData/DETALLES.md)**

