# Introducción

## Visión general del proyecto  
Este proyecto implementa un **pipeline de datos de aviación** siguiendo el patrón **Medallion (Raw -> Bronze -> Silver -> Gold)** para capturar, limpiar, modelar y analizar información procedente de **OpenSky Network** (telemetría de aeronaves y vuelos), complementada con **datasets estáticos** (aeropuertos de la Península Ibérica y prefijos de aerolíneas).  

El objetivo final es disponer de un **histórico propio y trazable**, preparado para **analítica avanzada** y visualizado mediante un **modelo semántico en Power BI**.  



## Alcance y datasets  
- **OpenSky – `/states/all`** cada 90 segundos (telemetría).  
- **OpenSky – `/flights/{arrival|departure}`** cada 12 horas por aeropuerto.  
- **Referencias estáticas**: aeropuertos de la Península Ibérica y prefijos de aerolíneas.  
- **Modelo final Gold**:  
  - Hechos: `fact_flights`, `fact_states`  
  - Dimensiones: `dim_airports`, `dim_prefixes`, `dim_airlines`, `dim_dates`  



## Arquitectura (Medallion en síntesis)

- **Raw**: snapshots **append-only** (NDJSON + Gzip) y CSV estáticos — sin cambios semánticos.  
- **Bronze**: aterrizaje en **Delta Lake** con **metadatos de ingesta** y manifiesto de control.  
- **Silver**: **limpieza, normalización, deduplicación** y claves de negocio estables.  
- **Gold**: **hechos/dimensiones** preparados para analítica y export a **Power BI** (Parquet).  
- **Power BI**: modelo estrella, medidas DAX y dashboard de validación.  

![Arquitectura](/docs/imgs/Arquitectura.png.png)



## Selección de tecnologías

## Selección de tecnologías

| Tecnología         | Rol en el proyecto                                | Justificación                                  |
|--------------------|---------------------------------------------------|-----------------------------------------------|
| **Python**         | Jobs de ingesta y utilidades                      | Ecosistema maduro para scripting y APIs       |
| **Apache Spark 3.5** | Procesamiento distribuido y transformaciones     | Escala, expresividad y soporte Delta Lake     |
| **Delta Lake**     | Formato de tablas transaccionales                  | ACID, *time travel*, soporte `MERGE`          |
| **Airflow**        | Orquestación de DAGs                              | Scheduling, dependencias, observabilidad      |
| **Docker Compose** | Entorno reproducible (Spark + Airflow + volúmenes) | Cohesión y despliegue portable                |
| **Parquet**        | Export a BI                                        | Columnar, compresión, integración Power BI    |
| **Power BI**       | Modelo semántico y visualización                   | Relacional (estrella), DAX, dashboards        |




## Buenas prácticas adoptadas

### Diseño de datos y linaje
- Separación clara por capas (**Raw, Bronze, Silver, Gold**).  
- Metadatos estandarizados (`_ingest_*`, `_silver_*`, `_gold_*`, `download_*`).  
- **Manifiestos** por capa para idempotencia y control de reprocesos.  
- Claves de negocio estables (`flight_id`, `(icao24,time_ts)`).  

### Calidad y gobernanza
- Métricas de calidad (duplicados, unknowns, freshness, outliers).  
- Deduplicación determinística con prioridad al último `_ingest_ts`.  
- Hashes de snapshots CSV para detectar cambios reales.  

### Rendimiento
- Proyección mínima de columnas y filtros con *pushdown*.  
- Particionado por fechas de evento en Silver y Gold.  
- `MERGE INTO` en upserts; `overwrite` solo para snapshots/dimensiones.  

### Orquestación y operación
- DAGs modulares (por dataset y capa) con dependencias claras.  
- Variables parametrizadas por entorno.  
- Aislamiento de responsabilidades entre descarga, aterrizaje, limpieza y modelado.  

### Observabilidad
- Tablas de métricas (`*_metrics`) y auditoría (`*_runs`, `processed_files`).  
- Esquema y naming consistentes (`snake_case`, sufijos `_utc`, `_ts`, `_date`).  

### Seguridad
- Gestión de tokens con `token_manager.py` (caché y refresco).  
- Volúmenes Docker con permisos controlados.  
- Fuentes de datos usadas bajo sus licencias (OpenSky, Wikipedia).  

### Power BI
- Modelo estrella: hechos (`fact_*`) y dimensiones (`dim_*`).  
- Una sola relación activa para aeropuertos, la otra gestionada con `USERELATIONSHIP`.  
- Tabla `_Measures` centraliza cálculos DAX.  



## Lineage de datos

El linaje asegura **trazabilidad de extremo a extremo**:  
- **Raw -> Bronze**: snapshots crudos aterrizados en Delta con metadatos de ingesta.  
- **Bronze -> Silver**: normalización, limpieza, deduplicación y claves de negocio.  
- **Silver -> Gold**: modelado en hechos y dimensiones listos para analítica.  
- **Gold -> Power BI**: export a Parquet y construcción del modelo semántico.  

Cada transición conserva metadatos heredados y añade nuevas marcas de versión (`_silver_version`, `_gold_version`), asegurando auditoría completa.  



## Estructura del repositorio

```
opensky-lambda-pipeline/
├── raw-data/                 # Snapshots crudos + CSV estáticos
├── lakehouse/                # Delta (bronze/silver/gold) + pbi_exports
├── spark_jobs/               # Scripts Spark (bronze/silver/gold/export)
├── dags/                     # Airflow DAGs por capa
├── utils/                    # Helpers de sesión Spark, manifiestos, métricas
├── docker-compose.yml        # Stack local (Spark, Airflow, volúmenes)
├── requirements.txt          # Dependencias Python
└── docs/                     # Imágenes del modelo y capturas de Power BI
```



## KPIs y preguntas objetivo  

El pipeline responde a preguntas de negocio iniciales como:  
- **Volumen de vuelos** por aeropuerto (salidas y llegadas).  
- **Top rutas (O–D)** y tiempo de bloque medio (`AVG block_time_s`).  
- **Participación por aerolínea** (% de vuelos totales).  
- **Tendencias temporales** (día, semana, mes).  
- **Cobertura y calidad** de estados (`coverage_pct`, outliers en velocidad/altitud).  
- **Unknowns** (aeropuertos no mapeados) como indicador de *data gaps*.  

# Cómo replicar el proyecto (Windows + WSL2 + Docker Desktop)

> **Resumen**: estos pasos te dejan el stack listo (Spark + Airflow + Lakehouse) para ejecutar **Raw -> Bronze -> Silver -> Gold -> Power BI** igual que en el repo.



## 1) Requisitos

- **Windows 10/11** con **WSL2** (Ubuntu recomendado).
- **Docker Desktop** para Windows (con backend **WSL2**).
- **Git** y **Python 3.10+** (opcional si vas a ejecutar scripts fuera de Docker).
- **Power BI Desktop** (para el modelo semántico).
- Cuenta en **OpenSky** (si usas OAuth o límites elevados).



## 2) Preparar WSL2 y Docker Desktop

### 2.1 Instalar/activar WSL2
```powershell
wsl --install -d Ubuntu
# o, si ya tienes WSL1:
wsl --set-default-version 2
```
Abre Ubuntu, crea tu usuario y actualiza:
```bash
sudo apt update && sudo apt upgrade -y
```

### 2.2 Ajustes en Docker Desktop
- **General ->**  *Use the WSL 2 based engine*  
- **Resources -> WSL Integration ->** activa tu distro (Ubuntu).  
- **Resources -> Advanced ->** asigna al menos **4 CPU**, **8–12 GB RAM**, **20+ GB** de disco.  
- **Resources -> File Sharing ->** asegúrate de que tu carpeta del proyecto está accesible (si trabajas en `C:\...`).

> **Recomendado**: clona el repo **dentro del filesystem de WSL** (por ejemplo `/home/tuusuario/dev/opensky-lambda-pipeline`) para evitar penalizaciones de I/O sobre `C:\`.



## 3) Clonar el repositorio
```bash
# Dentro de WSL (Ubuntu)
mkdir -p ~/dev && cd ~/dev
git clone <URL_DEL_REPO> opensky-lambda-pipeline
cd opensky-lambda-pipeline
```



## 4) Variables de entorno (`.env`)

Crea un archivo `.env` en la raíz del proyecto con algo como:

```dotenv
# Imágenes y red
SPARK_IMAGE=custom-spark:3.5
DOCKER_NETWORK=opensky-lambda-pipeline_default

# Rutas host -> contenedores
HOST_RAW=./raw-data
HOST_LAKEHOUSE=./lakehouse
HOST_SPARK_JOBS=./spark_jobs
HOST_AIRFLOW_DAGS=./dags

# Bounding box (Península Ibérica aprox.)
BBOX_MIN_LON=-10.0
BBOX_MIN_LAT=35.0
BBOX_MAX_LON=4.0
BBOX_MAX_LAT=45.0

# Frecuencias
STATES_EVERY_SECONDS=90
FLIGHTS_EVERY_HOURS=12

# OpenSky (si aplican OAuth/usuario)
OPENSKY_CLIENT_ID=...
OPENSKY_CLIENT_SECRET=...
OPENSKY_USERNAME=...
OPENSKY_PASSWORD=...

# Control de versiones lógicas
SILVER_VERSION=v1.0.0
GOLD_VERSION=v1.0.0

# UID/GID para permisos (bitnami spark suele usar 1001/0 o 1001/1001; en nuestro stack usamos 50000:0)
CONTAINER_UID=50000
CONTAINER_GID=0
```

> Ajusta `CONTAINER_UID/GID` para que los contenedores puedan escribir en `./lakehouse` y `./raw-data`.  



## 5) Permisos y volúmenes (WSL)

Crea las carpetas y dales permisos compatibles con el UID/GID del contenedor:

```bash
mkdir -p raw-data lakehouse
sudo chown -R $USER:$USER raw-data lakehouse
# Si el contenedor escribe como 50000:0, puedes permitir escritura amplia:
chmod -R 777 raw-data lakehouse
```

> Si ves mensajes como `setfacl: Not supported` en WSL, no pasa nada: **usa `chmod/chown`** en su lugar. ACL no es crítico aquí.

**Cache Ivy para Spark (recomendado):** en `docker-compose.yml` debe existir un volumen:
```yaml
# ejemplo
volumes:
  ivy_cache:
services:
  spark-worker:
    volumes:
      - ivy_cache:/opt/spark/.ivy2
  spark-master:
    volumes:
      - ivy_cache:/opt/spark/.ivy2
```
Así evitas descargar paquetes de Spark cada vez.



## 6) Levantar el stack

```bash
# Arranca todo
docker compose up -d
# (opcional) Inicializa estructura de lakehouse, si el repo incluye un servicio init:
docker compose up --exit-code-from init-lakehouse init-lakehouse
```

Verifica contenedores:
```bash
docker compose ps
```

Airflow UI: **http://localhost:8080**  
Spark History (si está configurado): **http://localhost:18080**  
Spark Master UI: **http://localhost:8081**



## 7) Configurar Airflow

1. Entra a la UI, usuario/clave por defecto del stack (consúltalo en el `docker-compose.yml`).  
2. Revisa **Variables** (ENV propagadas a DAGs): rutas `HOST_*`, versiones `SILVER_VERSION/GOLD_VERSION`, BBOX, etc.  
3. **Activa** los DAGs en orden:
   - `ingest_opensky_states_raw` (cada 90 s)  
   - `ingest_opensky_flights_raw` (cada 12 h)  
   - `bronze_states`, `bronze_flights`, `bronze_static`  
   - `silver_states`, `silver_flights`, `silver_static`  
   - `gold_dims`, `gold_facts`  
4. Verifica en **Graph/Logs** que los jobs terminan en `success` y que las tablas Delta aparecen en `./lakehouse`.

> Si usas credenciales OpenSky, crea una **Connection** o exporta variables (`OPENSKY_*`) que consuma tu `token_manager.py`.



## 8) Export a Power BI

Genera snapshots Parquet de la capa Gold:
```bash
docker compose exec spark-master bash -lc \
"spark-submit /opt/spark/spark_jobs/gold_export_pbi_job.py \
  --gold-root /opt/spark/lakehouse/gold \
  --out-root  /opt/spark/lakehouse/pbi_exports \
  --tables fact_flights fact_states dim_airports dim_airlines dim_dates dim_airlines_prefixes \
  --single-file"
```

En **Power BI Desktop**:  
- **Obtener datos -> Parquet** y selecciona cada carpeta de `/lakehouse/pbi_exports/<tabla>/`.  
- Crea relaciones y medidas DAX (ver sección de Power BI en el README).



## 9) Troubleshooting rápido

- **Permission denied** al escribir en `lakehouse`/`raw-data`  
  - Asegúrate de que existen y tienen permisos (`chmod -R 777`).  
  - Ajusta `CONTAINER_UID/GID` o añade `user: "50000:0"` en servicios Spark/Airflow.

- **`setfacl: Not supported` en WSL**  
  - Ignorable; usa `chown/chmod`. Quita pasos que usan `setfacl` del init si no son críticos.

- **Spark baja muy lento por dependencias**  
  - Añade volumen `ivy_cache` a `/opt/spark/.ivy2`.  
  - Mantén el contenedor vivo para cachear paquetes.

- **Airflow no programa en paralelo**  
  - Revisa `parallelism`, `dag_concurrency` y número de **workers**/executor.

- **`PATH_NOT_FOUND` en manifiestos**  
  - Asegúrate de ejecutar `ensure_manifest()` en los jobs; se crean automáticamente.

- **Filesystem Windows vs WSL**  
  - Preferible trabajar **dentro de WSL** (`/home/...`). Si montas `C:\...`, puede haber latencia de I/O.

- **Puertos ocupados (8080, 7077, 18080)**  
  - Cambia `ports` en `docker-compose.yml` o libera los puertos.



## 10) Flujo mínimo de validación (E2E)

1. **Arranca el stack** y activa los DAGs Raw -> Bronze -> Silver.  
2. Ejecuta **Gold** (dims -> facts).  
3. Lanza **export a Power BI** a `/lakehouse/pbi_exports`.  
4. Abre **Power BI**, conecta Parquet, crea relaciones y valida KPIs básicos:  
   - `Flights`, `Avg Block Time (min)`, `Coverage %`, `p95 Altitude/Velocity`.  

> Si todo ok, ya tienes el **Medallion** replicado y el **modelo semántico** funcionando.

# Fase 0 (Preparación del entorno)  

## Explicación lógica de la Fase 0  
Antes de empezar a capturar y transformar datos, necesitamos preparar un **entorno reproducible** que permita ejecutar de forma consistente todos los servicios del pipeline (**Airflow, Spark, Postgres, Lakehouse**).  

Los objetivos son:  
- **Configurar permisos y volúmenes** (especialmente `lakehouse` y `raw-data`).  
- **Orquestar servicios en Docker Compose** con inicialización de ACLs, Postgres para Airflow y un clúster Spark (master/worker/history).  
- **Homogeneizar UID/GID** entre host y contenedores (usamos `50000:0`).  
- **Garantizar reproducibilidad** en cualquier equipo con **WSL2 + Docker Desktop**.  

De esta forma, aseguramos que todo el equipo pueda ejecutar el pipeline con el mismo stack y sin problemas de permisos o dependencias.  



## Ficha técnica de la fase Preparación  

| Atributo             | Descripción |
|--|-|
| **Definición**        | Infraestructura base con Airflow + Spark + Postgres + Lakehouse en Docker Compose. |
| **Objetivo**          | Garantizar un entorno estable, con permisos correctos, cache de dependencias y orquestación lista. |
| **Tipo de objetos**   | Contenedores (Docker), volúmenes persistentes, directorios montados. |
| **Método de carga**   | `docker compose up -d` con inicialización automática (`init-lakehouse`). |
| **Transformaciones**  | Ninguna (fase puramente de infraestructura). |
| **Audiencia objetivo**| Ingenieros de datos que desarrollan/ejecutan el pipeline. |



## Componentes principales  

- **Servicio `init-lakehouse`**  
  Contenedor Alpine que ajusta permisos, crea directorios y aplica ACLs en `lakehouse` e `ivy_cache`.  

- **Servicio `postgres`**  
  Base de datos para Airflow (metadatos de DAGs).  

- **Servicio `airflow`**  
  Orquesta DAGs con `LocalExecutor`, incluye dependencias adicionales (`apache-airflow-providers-docker`, `psycopg2-binary`, etc.).  

- **Servicio `spark-master`**  
  Nodo maestro de Spark. Expone UI en `localhost:8081`.  

- **Servicio `spark-worker`**  
  Nodo worker de Spark. Conectado al master (`spark://spark-master:7077`).  

- **Servicio `spark-history`**  
  Servidor de historial de jobs Spark. Expone UI en `localhost:18080`.  

- **Volúmenes persistentes**  
  - `pg_data` -> datos de Postgres.  
  - `spark_events` -> logs de Spark para el History Server.  
  - `ivy_cache` -> cache de dependencias Spark (para acelerar builds).  



## Archivos implicados en la Fase 0  

```
opensky-lambda-pipeline/
│
├── docker-compose.yml         # Orquestación de servicios (init-lakehouse, airflow, spark, postgres)
├── .env                       # Variables de entorno (paths, credenciales, versiones)
├── lakehouse/                 # Carpeta montada para tablas Delta (permisos 50000:0)
├── raw-data/                  # Carpeta montada para snapshots crudos
├── dags/                      # DAGs de Airflow
├── spark_jobs/                # Jobs Spark
└── utils/                     # Helpers compartidos
```



## Variables de entorno necesarias (`.env`)  

```dotenv
TZ=Europe/Madrid
SPARK_IMAGE=custom-spark:3.5
SPARK_MASTER=spark://spark-master:7077

HOST_RAW=./raw-data
HOST_LAKEHOUSE=./lakehouse
HOST_UTILS=./utils
HOST_PYTHON_JOBS=./spark_jobs
HOST_SPARK_JOBS=./spark_jobs

SILVER_VERSION=v1.0.0
GOLD_VERSION=v1.0.0
```

> Añadir credenciales `OPENSKY_*` si usas OAuth o acceso autenticado.  



## Inventario de servicios (infraestructura base)

| Servicio         | Imagen              | Rol                        | Puerto     |
|------------------|---------------------|----------------------------|------------|
| `init-lakehouse` | alpine:3.20         | ACLs y permisos iniciales  | —          |
| `postgres`       | postgres:14         | DB metadatos Airflow       | —          |
| `airflow`        | apache/airflow:2.10 | Orquestador DAGs           | 8080 (UI)  |
| `spark-master`   | custom-spark:3.5    | Maestro Spark              | 7077, 8081 |
| `spark-worker`   | custom-spark:3.5    | Worker Spark               | 8082       |
| `spark-history`  | custom-spark:3.5    | History Server             | 18080      |



# Fase 1 (Raw Data)  

## Explicación lógica de la Fase 1  
En esta primera fase no buscamos todavía transformar ni limpiar datos, sino algo más básico pero fundamental: **asegurarnos de no perder información**.  

La API de OpenSky solo expone datos en tiempo real, sin histórico. Eso significa que, si no capturamos los datos en el momento exacto, se pierden para siempre.  

Por eso, hemos montado un sistema que hace lo siguiente:  
- **Cada 90 segundos** preguntamos a la API por el estado de todos los aviones que sobrevuelan la Península Ibérica (posición, altitud, velocidad…).  
- **Cada 12 horas** consultamos a la API las llegadas y salidas de los aeropuertos peninsulares listados en un CSV.  
- Todo lo que recibimos lo guardamos en ficheros crudos (`raw-data`), tal cual nos llega, sin añadir metadatos ni modificar nada. La única organización que imponemos es **guardar por carpetas de fecha**, para poder localizar los datos fácilmente más adelante.  

De esta manera conseguimos un **histórico propio y completo**: aunque OpenSky no nos lo dé, nosotros lo reconstruimos.  

Más adelante, en fases Bronze/Silver/Gold, ya limpiaremos, estructuraremos y analizaremos esta información.  



## Ficha técnica de la fase Raw

| Atributo             | Descripción |
|--|-|
| **Definición**        | Datos crudos sin procesar, exactamente como se obtienen desde la API de OpenSky o de los ficheros estáticos (Wikipedia). |
| **Objetivo**          | Garantizar **trazabilidad y depuración**: disponer de snapshots íntegros para auditar y repetir procesos posteriores. |
| **Tipo de objetos**   | - Semiestructurados (JSON NDJSON comprimido con Gzip). <br> - Estáticos en CSV (`peninsular_airports.csv`, `airlines_prefixes.csv`). |
| **Método de carga**   | *Append-only*: se añaden snapshots periódicos, nunca se sobreescriben. |
| **Transformaciones**  | Ninguna (solo compresión y particionado por fecha). |
| **Modelado de datos** | Ninguno (estructura cruda, sin esquema impuesto). |
| **Audiencia objetivo**| Ingenieros de datos. |



## Componentes principales  

- **`raw-data/airports/peninsular_airports.csv`**  
  CSV estático con los aeropuertos de la Península Ibérica (ICAO, IATA, nombre, etc.).  
  Usado por los DAGs para decidir qué aeropuertos consultar cada 12h.  
  Descargado de Wikipedia y limpiado a mano.  

- **`raw-data/airlines/airlines_prefixes.csv`**  
  CSV estático con prefijos de compañías aéreas.  
  Utilizado para enriquecer identificadores de vuelos en fases posteriores.  

- **`utils/token_manager.py`**  
  Gestiona el **OAuth2** con OpenSky. Pide tokens, los cachea y los renueva al expirar.  

- **`utils/airports.py`**  
  Función para cargar la lista de ICAOs desde el CSV.  

- **`python_jobs/opensky_client.py`**  
  Cliente genérico de la API de OpenSky. Incluye:  
  - Descarga de `/states/all` -> guardado como **NDJSON comprimido (gzip)** cada 90 s.  
  - Descarga de `/flights/arrival` y `/flights/departure` por aeropuerto -> guardado cada 12 h.  
  - Organización en carpetas particionadas por fecha.  
  - CLI para ejecución manual.  

- **`dags/ingest_opensky_states_raw.py`**  
  DAG de Airflow que lanza cada **90 segundos** la ingesta de `states` (bounding box Península).  

- **`dags/ingest_opensky_flights_raw.py`**  
  DAG de Airflow que cada **12 horas** recorre los aeropuertos del CSV y descarga arrivals + departures.  



## Archivos implicados en la fase 1

```
opensky-lambda-pipeline/
│
├── dags/                         
│   ├── ingest_opensky_states_raw.py     # DAG para /states/all (cada 90 s)
│   └── ingest_opensky_flights_raw.py    # DAG para arrivals/departures (cada 12 h)
│
├── python_jobs/                   
│   └── opensky_client.py               # Cliente API OpenSky (ingestas crudas)
│
├── utils/                         
│   ├── token_manager.py                # Gestión OAuth2 (tokens y refresco)
│   └── airports.py                     # Carga de ICAOs desde CSV
│
├── raw-data/                      
│   ├── airports/peninsular_airports.csv  # Lista estática de aeropuertos
│   ├── airlines/airlines_prefixes.csv    # Prefijos de aerolíneas
│   ├── states/date=YYYY-MM-DD/*.json.gz  # Snapshots cada 90s
│   ├── flights_arrival/tag=ICAO/date=YYYY-MM-DD/*.json.gz     # Arrivals cada 12h
│   └── flights_departure/tag=ICAO/date=YYYY-MM-DD/*.json.gz   # Departures cada 12h
│
├── docker-compose.yml                 # Orquestación de servicios
├── requirements.txt                   # Dependencias Python
└── README.md                          # Documentación
```



## Campos de cada dataset

### `peninsular_airports.csv`  
**Campos:**  
- `city`, `region`, `icao`, `iata`, `name`, `type`  

### `airlines_prefixes.csv`  
**Campos:**  
- `prefix`, `airline`  

### `states` (snapshot cada 90s de `/states/all`)  
**Campos principales:**  
- `icao24`, `callsign`, `origin_country`, `time_position`, `last_contact`,  
- `longitude`, `latitude`, `geo_altitude`, `baro_altitude`,  
- `velocity`, `heading`, `vertical_rate`, `on_ground`, …  

### `flights_departures` (cada 12h de `/flights/departure`)  
**Campos principales:**  
- `icao24`, `firstSeen`, `estDepartureAirport`, `lastSeen`, `estArrivalAirport`, `callsign`  

### `flights_arrivals` (cada 12h de `/flights/arrival`)  
**Campos principales:**  
- `icao24`, `firstSeen`, `estDepartureAirport`, `lastSeen`, `estArrivalAirport`, `callsign`  



## Inventario de datasets (Raw Zone)

| Dataset               | Formato       | Frecuencia     | Particionado                 | Fuente      |
|-----------------------|---------------|----------------|------------------------------|-------------|
| `peninsular_airports` | CSV estático  | Manual (única) | No                           | Wikipedia   |
| `airlines_prefixes`   | CSV estático  | Manual (única) | No                           | Manual      |
| `states`              | NDJSON + Gzip | Cada 90 s      | `date=YYYY-MM-DD`            | OpenSky API |
| `flights_arrivals`    | NDJSON + Gzip | Cada 12 h      | `tag=ICAO/date=YYYY-MM-DD`   | OpenSky API |
| `flights_departures`  | NDJSON + Gzip | Cada 12 h      | `tag=ICAO/date=YYYY-MM-DD`   | OpenSky API |




# Fase 2 (Bronze)

## Explicación lógica de la Fase 2
En Bronze **aterrizamos** los ficheros crudos en tablas **Delta Lake** sin transformar su semántica.  

Objetivos clave:

- **Trazabilidad & reproducibilidad**: añadimos metadatos de ingesta (`_ingest_id`, `_ingest_ts`, `_source_file`, …) y derivamos **download_ts / download_date** desde el nombre de fichero/carpeta.
- **Idempotencia**: usamos un **manifiesto Delta** global (`/lakehouse/meta/processed_files`) que evita reprocesar el mismo RAW (o el mismo snapshot CSV) dos veces.
- **Layout eficiente**: escribimos en **Delta** particionando por `download_date` (no por `tag`); `tag` se queda como **columna** para filtrar/derivar en Silver.
- **Static snapshots** (CSV) con **hash MD5**: si el fichero no cambia, **skip** automático.
- **Auditoría**: registramos métricas simples por ejecución en /lakehouse/meta/bronze_metrics.

> En Bronze **no limpiamos** (eso es Silver). Solo **normalizamos metadatos + layout** para lectura eficiente y control de cambios.



## Ficha técnica de la fase Bronze

| Atributo               | Descripción |
|-------------------------|-------------|
| **Definición**          | Normalización mínima de RAW a **Delta Lake** con metadatos estandarizados de ingesta. |
| **Objetivo**            | Trazabilidad, idempotencia y performance de lectura/particionado. |
| **Tipo de objetos**     | Delta tables (parquet + transacciones). |
| **Método de carga**     | `append` (incrementales como states/flights) o `overwrite` (snapshots CSV). |
| **Transformaciones**    | Solo **metadatos**: `_ingest_id`, `_ingest_ts`, `_source_file`, `_ingestion_date`, `download_ts`, `download_date`; y **`tag`** (para flights departures) extraído del path. |
| **Particionado**        | `download_date` en todos los incrementales. Los estáticos no se particionan. |
| **Control idempotencia**| Tabla Delta **manifiesto** con `(dataset, source_file, status, file_hash?)`. |
| **Audiencia objetivo**  | Ingenieros de Datos. |




## Componentes principales

### Utils (`/utils`)
- **`spark_session.py`** -> Crea la **SparkSession** (app name configurable). Activa soporte Delta.
- **`bronze_utils.py`** -> Helpers de metadatos (`add_common_metadata`) y escritura (`write_delta`).
- **`init_bronze_manifest.py`** -> Gestión del **manifiesto Delta**:  
  - `ensure_manifest`, `discover_new_files`, `update_manifest` (para incrementales).  
  - `snapshot_needs_processing`, `compute_file_hash` (para snapshots CSV).
- **`bronze_metrics.py`** -> **Auditoría ligera** (métricas por ejecucion):
  - `ensure_bronze_metrics_table(spark)`
  - `capture_metrics(df, dataset, source_file, ingest_id, exclude_cols=None, rows_in=None)`
  - `write_metrics(spark, metrics, rows_out=None, status="ok", notes="")`

### Spark Jobs (`/spark_jobs`)
- **`bronze_states_job.py`** -> NDJSON `states` -> Delta `/bronze/states` particionado por `download_date`.  
- **`bronze_flights_arrival_job.py`** -> NDJSON `flights_arrival` -> Delta `/bronze/flights_arrival`.  
- **`bronze_flights_departure_job.py`** -> NDJSON `flights_departure` -> añade columna `tag` -> Delta `/bronze/flights_departure`.  
- **`bronze_static_job.py`** -> Snapshots estáticos (`airlines_prefixes`, `airports`) -> Delta con `overwrite` y control por hash.

### DAGs (`/dags`)
- **`bronze_states.py`** -> Orquesta ingesta de `states`.  
- **`bronze_flights.py`** -> Orquesta ingesta de `flights_arrival` + `flights_departure`.  
- **`bronze_static.py`** -> Orquesta ingesta de estáticos (`airlines_prefixes`, `airports`).



## Archivos implicados en la Fase 2

```
opensky-lambda-pipeline/
│
├── utils/
│   ├── spark_session.py
│   ├── bronze_utils.py
│   ├── bronze_metrics.py         
│   └── init_bronze_manifest.py
│
├── spark_jobs/
│   ├── bronze_states_job.py
│   ├── bronze_flights_arrival_job.py
│   ├── bronze_flights_departure_job.py
│   └── bronze_static_job.py
│
└── dags/
    ├── bronze_states.py
    ├── bronze_flights.py
    └── bronze_static.py
```



## Campos por dataset (Bronze)

> **Nota**: Bronze **no limpia** ni renombra; solo añade metadatos y, en `flights_departure`, la columna `tag`.

### `bronze.states`
- **Originales**: `icao24`, `callsign`, `origin_country`, `time_position`, `last_contact`, `longitude`, `latitude`, `baro_altitude`, `on_ground`, `velocity`, `true_track`, `vertical_rate`, `sensors`, `geo_altitude`, `squawk`, `spi`, `position_source`, `category`, `_corrupt_record`.  
- **Metadatos añadidos**: `_ingest_id`, `_ingest_ts`, `_source_file`, `_ingestion_date`, `download_ts`, `download_date` (partición).

### `bronze.flights_arrival`
- **Originales**: `icao24`, `firstSeen`, `estDepartureAirport`, `lastSeen`, `estArrivalAirport`, `callsign`, distancias/horiz/vert, candidatesCount, `_corrupt_record`.  
- **Metadatos**: igual que `states`.

### `bronze.flights_departure`
- **Originales**: mismos campos que arrivals.  
- **Extras Bronze**: `tag` (ej: `LEMD`) extraído del path.  
- **Metadatos**: igual que `states`.

### `bronze.ref_airlines_prefixes`
- **Originales**: `prefix`, `airline`.  
- **Metadatos**: metadatos estándar (`download_date` = fecha ingesta).

### `bronze.ref_airports`
- **Originales**: `city`, `region`, `icao`, `iata`, `name`, `type`.  
- **Metadatos**: metadatos estándar (`download_date` = fecha ingesta).



## Inventario de datasets (Bronze Zone)

| Tabla                           | Carga     | Partición       | Manifiesto | Notas                                |
|---------------------------------|-----------|-----------------|------------|--------------------------------------|
| `bronze.states`                 | Append    | `download_date` | Sí         | Incremental cada 1h.                 |
| `bronze.flights_arrival`        | Append    | `download_date` | Sí         | Incremental cada 12h.                |
| `bronze.flights_departure`      | Append    | `download_date` | Sí         | Incremental cada 12h; añade `tag`.   |
| `bronze.ref_airlines_prefixes`  | Overwrite | —               | Sí (hash)  | Semanal snapshots CSV; skip si no cambia. |
| `bronze.ref_airports`           | Overwrite | —               | Sí (hash)  | Semanal snapshots CSV; skip si no cambia. |




## Flujo de procesamiento (resumen)

1. **Descubrimiento**  
   - Incrementales -> `discover_new_files()` contra manifiesto.  
   - Snapshots CSV -> `snapshot_needs_processing()` (MD5).
2. **Lectura + esquema**  
   - Lectura **esquemática** con modo `PERMISSIVE`.  
3. **Metadatos Bronze**  
   - `add_common_metadata()` (+ `tag` para departures).  
4. **Escritura Delta**  
   - Incrementales -> `append` particionando por `download_date`.  
   - Snapshots -> `overwrite`.  
5. **Registro opcional**  
   - `CREATE TABLE ... USING delta LOCATION ...`.  
6. **Actualizar manifiesto**  
   - `update_manifest()` con `status='done'` y `file_hash` si aplica.



## Ejecución (ejemplos)

```bash
# States
spark-submit ... bronze_states_job.py   --input /opt/spark/raw-data/states   --output /opt/spark/lakehouse/bronze/states   --register-table

# Flights
spark-submit ... bronze_flights_arrival_job.py   --input /opt/spark/raw-data/flights_arrival   --output /opt/spark/lakehouse/bronze/flights_arrival   --register-table

spark-submit ... bronze_flights_departure_job.py   --input /opt/spark/raw-data/flights_departure   --output /opt/spark/lakehouse/bronze/flights_departure   --register-table

# Estáticos
spark-submit ... bronze_static_job.py   --dataset airlines_prefixes   --input-file /opt/spark/raw-data/airlines/airlines_prefixes.csv   --output /opt/spark/lakehouse/bronze/ref_airlines_prefixes   --register-table
```



## Operación & troubleshooting

- **`Permission denied`** al escribir en Lakehouse -> revisar mounts y usuario (`50000:0`).  
- **`PATH_NOT_FOUND`** en manifiesto -> `ensure_manifest()` lo crea automáticamente.  
- **`WARN MergeIntoCommand`** -> inofensivo (doble escaneo interno de Delta).  
- **Paralelismo** en Airflow -> con 1 worker se ejecutan en serie; para paralelo, aumentar `parallelism` o más workers.

# Fase 3 (Silver)

## Explicación lógica de la Fase Silver
En **Silver** normalizamos y modelamos los datos de Bronze para dejarlos **consistentes, deduplicados y listos** para analítica (Gold). El output son tablas **Delta** con claves de *upsert* estables y sólo las columnas necesarias para los KPIs.

Objetivos clave:
- **Normalización** de cadenas (TRIM/UPPER/Initcap), timestamps (epoch->UTC) y derivadas de calendario.
- **Deduplicación determinística** con ventanas por clave priorizando mayor `_ingest_ts` (última observación).
- **Idempotencia y versionado** vía **manifiesto Silver** con `silver_version` y (para snapshots) `file_hash`.
- **Upsert transaccional** (`MERGE INTO`) usando **claves lógicas** (`(icao24, time_ts)` o `flight_id`).
- **Métricas de calidad** en `silver_metrics` (`dup_ratio`, `unknown_pct`, `freshness_lag_minutes`, `extras_json`).
- **Layout eficiente**: partición por **fecha de evento** (`*_date_utc`) para lectura analítica.

> A diferencia de Bronze, en Silver **sí limpiamos** y fijamos el **contrato de datos** que Gold consumirá.



## Ficha técnica de la Fase Silver

| Atributo                 | Descripción |
|--|-|
| **Definición**           | Transformación **limpia** de Bronze a **Delta** con claves de upsert estables y esquema mínimo para KPIs. |
| **Objetivo**             | Datos consistentes/deduplicados listos para métricas, con trazabilidad y versionado. |
| **Objetos**              | Tablas Delta (parquet + transacciones). |
| **Carga**                | `MERGE` (incrementales: `states`, `flights_*`) · `overwrite` (snapshots: `ref_*`). |
| **Transformaciones**     | TRIM/UPPER/Initcap, epoch->UTC, derivadas `*_date_utc`, `*_hour`, `route_key`, `flight_id`, *joins* a referencias, dedupe, selección mínima. |
| **Particionado**         | `states`: `event_date_utc` · `flights_arrival`: `arr_date_utc` · `flights_departure`: `dep_date_utc` · `ref_*`: sin partición. |
| **Idempotencia**         | **Manifiesto Silver** (`/lakehouse/meta/silver_runs`) con `(dataset, source_file, status, silver_version, file_hash?)`. |
| **Auditoría**            | **`/lakehouse/meta/silver_metrics`**; una fila por ejecución/dataset con KPIs de calidad. |
| **Versionado lógico**    | `SILVER_VERSION` (p.ej. `v1.0.0`) propagado a `_silver_version`. |
| **Audiencia**            | Data Engineers / Analytics Engineers. |



## Componentes principales (explicado)
### Utils (`/utils`)
- **`spark_session.py`**  
  Crea la `SparkSession` con soporte Delta activado. Permite inyectar el nombre de la app, rutas y *packages* desde env.

- **`silver_utils.py`**  
  - `add_silver_metadata(df, source_name, silver_version)` -> añade `_silver_version` y `_silver_ts` (renombrado desde `_processing_date` si aplica) y etiqueta `_source` opcional.
  - `write_delta_append(...)` (si lo usas) -> escritura APPEND con control de partición e inclusión de `_silver_version` como partición si interesa.

- **`init_silver_manifest.py`**  
  - `ensure_manifest(spark)` -> crea la Delta table del manifiesto si no existe.
  - `discover_new_files(spark, input_root, dataset, pattern)` -> listado de ficheros/particiones **no** procesados (`status='done'`) para ese dataset (independiente de `silver_version`).
  - `needs_processing(...)` -> *gate* de re-proceso por `(dataset, source_file, silver_version)`.
  - **Snapshots**: `snapshot_needs_processing(...)` calcula MD5 local y decide si re-procesar; `compute_file_hash(...)` expuesto.

- **`silver_metrics.py`**  
  - `ensure_silver_metrics_table(spark)` -> crea/valida la tabla Delta de métricas.
  - `capture_metrics(df, dataset, source_key, ingest_id, ...)` -> base (rows_in, nulls por columna, esquema, corrupt_rows).
  - **Extras**:  
    - `with_dup_ratio(metrics, duplicates_count, rows_in)`  
    - `with_unknown_pct(metrics, unknown_count, rows_out)`  
    - `with_freshness_lag_minutes(metrics, max_ingest_ts_col, df_out)`  
    - `with_extras(metrics, dict)` -> añade JSON arbitrario (p.ej., outliers).  
  - `write_metrics(spark, metrics, rows_out, status, notes)` -> append en Delta.

### Spark Jobs (`/spark_jobs`) — responsabilidades
- **`silver_states_job.py`**  
  Lee Bronze `states`, normaliza y valida coordenadas/velocidades, deriva calendario, deduplica por `(icao24,time_ts)`, y **MERGE** al Silver particionado por `event_date_utc`.
- **`silver_flights_arrival_job.py`**  
  Construye `flight_id`, enriquece con referencias Silver (airlines/airports), calcula calendarios y `block_time_s`, deduplica por `flight_id`, y **MERGE** en `arr_date_utc`.
- **`silver_flights_departure_job.py`**  
  Idéntico a arrivals, pero particionado por `dep_date_utc`.
- **`silver_static_job.py`**  
  Toma `ref_*` de Bronze, limpia y deduplica (`prefix`/`icao`), añade metadatos y escribe **snapshot overwrite** en Silver. Actualiza manifiesto con **hash de contenido** (independiente del orden físico).

### DAGs (`/dags`)
- **`silver_states.py`** (cada hora), **`silver_flights.py`** (diario) y **`silver_static.py`** (semanal) orquestan los *spark-submit* con *mounts*, *packages* y variables de entorno coherentes con Bronze.



## Archivos implicados en la Fase Silver
```
opensky-lambda-pipeline/
│
├── utils/
│   ├── spark_session.py
│   ├── silver_utils.py
│   ├── silver_metrics.py
│   └── init_silver_manifest.py
│
├── spark_jobs/
│   ├── silver_states_job.py
│   ├── silver_flights_arrival_job.py
│   ├── silver_flights_departure_job.py
│   └── silver_static_job.py
│
└── dags/
    ├── silver_states.py
    ├── silver_flights.py
    └── silver_static.py
```



## Inventario de datasets (Silver Zone)
| Tabla                          | Carga            | Partición        | **Clave MERGE**           | Manifiesto |
|--------------------------------|------------------|------------------|---------------------------|------------|
| `silver.states`                | Merge (upsert)   | `event_date_utc` | `(icao24, time_ts)`       | Sí         |
| `silver.flights_arrival`       | Merge (upsert)   | `arr_date_utc`   | `flight_id`               | Sí         |
| `silver.flights_departure`     | Merge (upsert)   | `dep_date_utc`   | `flight_id`               | Sí         |
| `silver.ref_airlines_prefixes` | Overwrite (snap) | —                | — (dedupe por `prefix`)   | Sí (hash)  |
| `silver.ref_airports`          | Overwrite (snap) | —                | — (dedupe por `icao`)     | Sí (hash)  |




## Campos por dataset (Silver) — **negocio vs metadatos**
> Como en Bronze, detallamos **metadatos heredados** y **metadatos añadidos** por Silver. Sólo persistimos lo necesario para KPIs y trazabilidad.

### `silver.states`
- **Claves/Partición**: MERGE por `(icao24, time_ts)` · partición `event_date_utc`.
- **Negocio**:
  - `icao24`, `time_ts`, `event_date_utc`, `hour_of_day`, `velocity_mps`, `geo_alt_m`,  
    `flag_velocity_outlier`, `flag_alt_outlier`
- **Metadatos heredados de Bronze (subset)**:
  - `_ingest_id`, `_ingest_ts`, `_source_file`, `download_ts`, `download_date`
- **Metadatos añadidos por Silver**:
  - `_silver_version`, `_silver_ts`

### `silver.flights_arrival`
- **Claves/Partición**: MERGE `flight_id` · partición `arr_date_utc`.
- **Negocio**:
  - `flight_id`, `icao24`, `callsign_norm`, `airline_prefix`, `airline_name`  
  - `dep_icao`, `dep_iata`, `arr_icao`, `arr_iata`, `unknown_dep`, `unknown_arr`  
  - `first_seen_ts`, `last_seen_ts`, `block_time_s`  
  - `dep_date_utc`, `arr_date_utc`, `dep_hour`, `arr_hour`, `dep_dow`, `arr_dow`, `dep_week`, `arr_week`, `dep_month`, `arr_month`  
  - `route_key`
- **Metadatos heredados de Bronze (subset)**:
  - `_ingest_ts`, `_source_file`, `download_ts`, `download_date`
- **Metadatos añadidos por Silver**:
  - `_silver_version`, `_silver_ts`

### `silver.flights_departure`
- **Claves/Partición**: MERGE `flight_id` · partición `dep_date_utc`.
- **Negocio**:
  - **Mismas columnas de negocio** que `flights_arrival` (cambiando la partición por `dep_date_utc`).
- **Metadatos heredados de Bronze (subset)**:
  - `_ingest_ts`, `_source_file`, `download_ts`, `download_date`
- **Metadatos añadidos por Silver**:
  - `_silver_version`, `_silver_ts`

### `silver.ref_airlines_prefixes`
- **Carga**: snapshot `overwrite` · dedupe por `prefix`.
- **Negocio**:
  - `prefix`, `airline_name`
- **Metadatos heredados de Bronze (subset)**:
  - `_ingest_ts`, `_source_file`, `download_ts`, `download_date`
- **Metadatos añadidos por Silver**:
  - `_silver_version`, `_silver_ts`

### `silver.ref_airports`
- **Carga**: snapshot `overwrite` · dedupe por `icao`.
- **Negocio**:
  - `city`, `region`, `icao`, `iata`, `name`, `type`
- **Metadatos heredados de Bronze (subset)**:
  - `_ingest_ts`, `_source_file`, `download_ts`, `download_date`
- **Metadatos añadidos por Silver**:
  - `_silver_version`, `_silver_ts`



## Flujo de procesamiento por dataset (detallado)

### `silver_states_job.py`
1. **Setup**  
   Lee env/CLI (`--input`, `--output`, `--pattern`, `--silver-version`, `--silver-run-id`). `ensure_manifest()` y `ensure_silver_metrics_table()`.
2. **Descubrimiento**  
   `discover_new_files(input_root, dataset='silver.states', pattern='download_date=*/*.parquet')` -> lista de paths **no** `done` en el manifiesto (cualquier versión).  
3. **Lectura Bronze**  
   `spark.read.format('parquet').option('basePath', input_root).load(files)`; se seleccionan sólo columnas usadas.  
4. **Normalización**  
   - `icao24 = UPPER(TRIM(icao24))` (vacío->NULL).  
   - `time_ts = to_utc_timestamp(to_timestamp(COALESCE(time_position,last_contact_normalizado)), 'UTC')`  
     - si epoch > 10^10, dividir entre 1000 (milisegundos).  
5. **Derivadas calendario**  
   - `event_date_utc = to_date(time_ts)` · `hour_of_day = hour(time_ts)`
6. **Validaciones / flags**  
   - `lat ∈ [-90, 90]`, `lon ∈ [-180, 180]` (fuera->NULL)  
   - `flag_velocity_outlier = (velocity_mps < 0) OR (velocity_mps > 350)`  
   - `flag_alt_outlier = (geo_alt_m < -500) OR (geo_alt_m > 20000)`  
   - `heading >= 360` -> `heading % 360`
7. **Filtro de clave**  
   Requiere `icao24` y `time_ts` **no nulos**.
8. **Deduplicación**  
   Ventana por **todas** las columnas de negocio (excluyendo metadatos de Bronze) o por clave `(icao24,time_ts)`; prioridad al mayor `_ingest_ts`.  
   Registra `before_dedupe`, `after_dedupe` para métricas.
9. **Metadatos Silver**  
   `add_silver_metadata(df, source_name='bronze.states', silver_version)` -> añade `_silver_version`, `_silver_ts`.
10. **Upsert a Delta**  
   `MERGE INTO silver.states AS t USING df AS s ON t.icao24=s.icao24 AND t.time_ts=s.time_ts`  
   - Partición: `event_date_utc`.  
   - `whenMatchedUpdateAll / whenNotMatchedInsertAll`.
11. **Métricas**  
   `capture_metrics(df, dataset='silver.states', source_key=f"batch://silver.states/{run_id}", ...)` +  
   `with_dup_ratio`, `with_freshness_lag_minutes("_ingest_ts", df)`, `with_extras({"velocity_outliers":..., "alt_outliers":...})` -> `write_metrics(...)`.
12. **Manifiesto**  
   `update_manifest(files, dataset='silver.states', ingest_id=run_id, status='done', silver_version)`.



### `silver_flights_arrival_job.py`
1. **Setup**  
   Paths Bronze/Silver, refs Silver (`ref_airlines_prefixes`, `ref_airports`), versión y run_id; ensure manifest/metrics table.
2. **Descubrimiento**  
   `discover_new_files(..., pattern='download_date=*/*.parquet')`.
3. **Lectura Bronze**  
   `option('basePath', input_root)`; se leen columnas de vuelos/tiempos y metadatos.
4. **Normalización**  
   - `icao24 = UPPER(TRIM(...))`  
   - `callsign_norm = UPPER(TRIM(callsign))` (vacío->NULL)  
   - `dep_icao = UPPER(TRIM(estDepartureAirport))` · `arr_icao = UPPER(TRIM(estArrivalAirport))`
5. **Tiempos y calendario**  
   - `first_seen_ts = to_utc_timestamp(to_timestamp(firstSeen_norm), 'UTC')` (manejo ms/seg)  
   - `last_seen_ts  = to_utc_timestamp(to_timestamp(lastSeen_norm),  'UTC')`  
   - `block_time_s = unix_timestamp(last_seen_ts) - unix_timestamp(first_seen_ts)` (>=0)  
   - Derivadas: `dep_date_utc/arr_date_utc`, `dep_hour/arr_hour`, `dep_dow/arr_dow`, `dep_week/arr_week`, `dep_month/arr_month`
6. **Derivadas negocio**  
   - `route_key = concat_ws('->', dep_icao, arr_icao)`  
   - `airline_prefix = regexp_extract(callsign_norm, '^[A-Z]{2,4}', 0)`  
   - `flight_id = sha2(concat_ws('|', icao24, dep_icao, arr_icao, first_seen_ts, last_seen_ts), 256)`
7. **Enriquecimiento**  
   - Join **`silver.ref_airlines_prefixes`** por `airline_prefix` -> `airline_name`  
   - Join **`silver.ref_airports`** por `dep_icao`/`arr_icao` -> `dep_iata`/`arr_iata`  
   - Flags: `unknown_dep/unknown_arr` si no hay mapeo
8. **Filtros y dedupe**  
   - Requiere `flight_id`, `first_seen_ts`, `last_seen_ts` no nulos; `block_time_s >= 0`  
   - Dedupe por `flight_id` (prioridad mayor `_ingest_ts`); registrar before/after
9. **Metadatos Silver**  
   Añade `_silver_version`, `_silver_ts` y selecciona columnas mínimas para KPIs.
10. **Upsert**  
   `MERGE INTO silver.flights_arrival ON t.flight_id = s.flight_id` · partición `arr_date_utc`.
11. **Métricas**  
   - `dup_ratio`  
   - `unknown_pct` sobre `(unknown_dep + unknown_arr > 0)` o bien suma por columna (decisión documentada)  
   - `freshness_lag_minutes` desde `_ingest_ts`  
   - `extras_json` con contadores (p.ej., `{"unknown_dep": X, "unknown_arr": Y}`)
12. **Manifiesto**  
   `update_manifest(..., dataset='silver.flights_arrival', status='done', silver_version)`.



### `silver_flights_departure_job.py`
**Igual a arrivals**, con estas diferencias:
- **Partición** del destino: `dep_date_utc`.
- `source_key` de métricas etiquetado como `silver.flights_departure`.
- Orden recomendado de orquestación: arrivals -> departures (para reportes que dependan de ambas).



### `silver_static_job.py`
1. **Setup**  
   `--dataset (airlines_prefixes|airports)`, rutas Bronze/Silver, versiones; ensure manifest/metrics.
2. **Lectura Bronze Delta**  
   - `airlines_prefixes`: `prefix`, `airline` + metadatos  
   - `airports`: `city`, `region`, `icao`, `iata`, `name`, `type` + metadatos
3. **Limpieza**  
   - **Airlines**: `prefix = UPPER(TRIM)`; regex `^[A-Z]{2,4}$`; `airline_name = Initcap(TRIM(...))`  
   - **Airports**: `icao = UPPER(TRIM)` + regex `^[A-Z0-9]{4}$` (obligatorio); `iata = UPPER(TRIM)` + regex `^[A-Z0-9]{3}$`; `city/name = Initcap`, `region = UPPER`, `type = lower`
4. **Deduplicación**  
   - Airlines: por `prefix` -> mayor `_ingest_ts`  
   - Airports: por `icao` -> mayor `_ingest_ts`
5. **Metadatos Silver**  
   `_silver_version`, `_silver_ts`
6. **Escritura snapshot**  
   `mode='overwrite'` sin partición en `silver.ref_*`
7. **Métricas**  
   - `dup_ratio` (before vs after)  
   - `unknown_pct` (p.ej., % `airline_name` nulo en airlines; % `iata` nulo en airports)  
   - `freshness_lag_minutes` desde `_ingest_ts`
8. **Manifiesto con hash de contenido**  
   Se calcula hash **estable** del contenido (orden lógico) y se llama `update_manifest(..., file_hash=<hash>, silver_version)`.



## Ejecución (ejemplos rápidos)

```bash
# States
spark-submit ... silver_states_job.py   --input  /opt/spark/lakehouse/bronze/states   --output /opt/spark/lakehouse/silver/states   --pattern 'download_date=*/*.parquet'   --silver-version v1.0.0 --register-table

# Flights: arrivals
spark-submit ... silver_flights_arrival_job.py   --input  /opt/spark/lakehouse/bronze/flights_arrival   --output /opt/spark/lakehouse/silver/flights_arrival   --ref-airlines /opt/spark/lakehouse/silver/ref_airlines_prefixes   --ref-airports /opt/spark/lakehouse/silver/ref_airports   --pattern 'download_date=*/*.parquet'   --silver-version v1.0.0 --register-table

# Flights: departures
spark-submit ... silver_flights_departure_job.py   --input  /opt/spark/lakehouse/bronze/flights_departure   --output /opt/spark/lakehouse/silver/flights_departure   --ref-airlines /opt/spark/lakehouse/silver/ref_airlines_prefixes   --ref-airports /opt/spark/lakehouse/silver/ref_airports   --pattern 'download_date=*/*.parquet'   --silver-version v1.0.0 --register-table

# Estáticos (snapshots)
spark-submit ... silver_static_job.py   --dataset airlines_prefixes   --bronze-input  /opt/spark/lakehouse/bronze/ref_airlines_prefixes   --silver-output /opt/spark/lakehouse/silver/ref_airlines_prefixes   --silver-version v1.0.0 --register-table

spark-submit ... silver_static_job.py   --dataset airports   --bronze-input  /opt/spark/lakehouse/bronze/ref_airports   --silver-output /opt/spark/lakehouse/silver/ref_airports   --silver-version v1.0.0 --register-table


```

# Fase 4 (Gold)

## Explicación lógica de la Fase Gold

En **Gold** materializamos el **modelo analítico** que consumirá BI (Power BI, notebooks, etc.). Partimos de Silver (ya limpio y deduplicado) y construimos:

- **Hechos**:
  - `fact_flights`: vuelos agregando salidas/llegadas con claves de negocio y enriquecimiento de aerolíneas/aeropuertos.
  - `fact_states`: agregaciones diarias por aeronave de la telemetría (`states`) con métricas de cobertura y calidad.
- **Dimensiones**:
  - `dim_airports`: catálogo de aeropuertos (ICAO/IATA/Nombre/Región/Tipo).
  - `dim_prefixes`: prefijos de aerolíneas (callsign -> aerolínea).
  - `dim_airlines`: aerolíneas **observadas** en los hechos (derivada).
  - `dim_dates`: calendario para joins temporales.

**Objetivos**: esquema estable para dashboards, idempotencia/versionado, buen rendimiento (particiones por fecha) y trazabilidad (metadatos `_silver_*` y `_gold_*`).

## Campos por dataset

### `gold.fact_flights`
**Negocio**  
`flight_id`, `icao24`, `callsign_norm`, `airline_prefix`, `airline_name`, `dep_icao`, `dep_iata`, `arr_icao`, `arr_iata`, `unknown_dep`, `unknown_arr`, `first_seen_ts`, `last_seen_ts`, `block_time_s`, `dep_date_utc`, `arr_date_utc`, `dep_hour`, `arr_hour`, `dep_dow`, `arr_dow`, `dep_week`, `arr_week`, `dep_month`, `arr_month`, `route_key`.

**Metadatos**  
`_ingest_ts`, `_source_file`, `download_ts`, `download_date`, `_silver_version`, `_silver_ts`, `_gold_version`, `_gold_ts`, `_gold_run_id`, `_gold_job`, `_source`.

### `gold.fact_states`
**Negocio**  
`icao24`, `event_date_utc`, `pings_count`, `p95_velocity`, `p95_altitude`, `pct_velocity_outlier`, `pct_alt_outlier`, `coverage_pct`.

**Metadatos**  
`_ingest_ts`, `_silver_ts`, `download_date`, `_gold_version`, `_gold_ts`, `_gold_run_id`, `_gold_job`, `_source`.

### `gold.dim_airports`
`icao`, `iata`, `name`, `city`, `region`, `type` + `_ingest_ts`, `download_date`, `_silver_version`, `_silver_ts`, `_gold_*`, `_source`.

### `gold.dim_prefixes`
`prefix`, `airline_name` + `_ingest_ts`, `download_date`, `_silver_*`, `_gold_*`, `_source`.

### `gold.dim_airlines`
`airline_prefix`, `airline_name` + `_gold_*`, `_source`.

### `gold.dim_dates`
`date`, `dow` (0=Lunes), `week`, `month (yyyy-MM)`, `quarter`, `year`, `is_weekend` + `_gold_*`, `_source`.

## Orquestación (Airflow)

**`dags/gold_dims.py`**
- Construye: `dim_airports` -> `dim_prefixes` -> `dim_airlines`.  
- `dim_dates` independiente.  
- Programación diaria (ej. 03:00).

**`dags/gold_facts.py`**
- Espera a `gold_dims` del día (ExternalTaskSensor).  
- Ejecuta `fact_flights` + `fact_states`.  
- Programación diaria (ej. 03:30).

## Ficha técnica

| Atributo        | Descripción |
|-----------------|-------------|
| Definición      | Capa analítica en Delta, derivada de Silver. |
| Objetivo        | Proveer hechos/dimensiones listos para KPIs con estabilidad de esquema. |
| Tipo de objetos | Delta tables. |
| Método de carga | Hechos: `merge`/`overwrite` según job. Dimensiones: `merge` (ref) u `overwrite` (derivadas/fechas). |
| Particionado    | `fact_flights`: `dep_date_utc` · `fact_states`: `event_date_utc` · Dimensiones: — |
| Versionado      | `GOLD_VERSION` + `_gold_version`, `_gold_ts`, `_gold_run_id`, `_gold_job`. |
| Auditoría       | `/lakehouse/meta/gold_metrics`. |
| Manifiesto      | `/lakehouse/meta/gold_runs`. |
| Audiencia       | Analytics Engineers / BI. |


## KPIs (Gold) – Mapeo rápido

| KPI / Visualización              | Columnas Gold                                   | Nota                       |
|----------------------------------|-------------------------------------------------|----------------------------|
| Vuelos por aeropuerto (arr/dep)  | `dep_icao`, `arr_icao`, `dep_date_utc`, `arr_date_utc` | Conteos.                   |
| Top rutas (O–D)                  | `route_key`, `flight_id`, `block_time_s`        | Conteo y `AVG(block_time_s)`. |
| Tiempo de bloque medio           | `block_time_s`                                  | Ya precalculado.           |
| Participación por aerolínea      | `airline_prefix`, `airline_name`                | % sobre total.             |
| Heatmap hora × día                | `dep_hour/arr_hour`, `dep_dow/arr_dow`          | Histogramas.               |
| Tendencia semanal/mensual        | `dep_week/arr_week`, `dep_month/arr_month`      | Series.                    |
| Cobertura States                 | `icao24`, `event_date_utc`, `pings_count`, `coverage_pct` | Medias/percentiles.        |
| Outliers States                  | `pct_velocity_outlier`, `pct_alt_outlier`, `event_date_utc` | % por día/ruta.            |
| Perfil altitud/velocidad (agg.)  | `p95_velocity`, `p95_altitude`, `event_date_utc` | Percentiles.               |
| Freshness (lag)                  | `_ingest_ts` / `_gold_ts`                       | `now() - max()`.           |
| Unknown airports                 | `unknown_dep`, `unknown_arr`                    | % filas con flag.          |
| Turnaround (v2)                  | `icao24`, `arr_date_utc`, `dep_date_utc`        | Ventanas por aeronave.     |


## Componentes principales

### Utils

-   `gold_utils.py` -> Helpers comunes para añadir metadatos y escribir
    tablas Gold.
-   `init_gold_manifest.py` -> Gestiona manifiesto de ejecuciones Gold.
-   `gold_metrics.py` -> Captura métricas de calidad y volumetría.

### Spark Jobs

-   `gold_build_fact_flights_job.py` -> Construye tabla de hechos
    `fact_flights`.
-   `gold_build_fact_states_job.py` -> Construye tabla de hechos
    `fact_states`.
-   `gold_build_dim_airports_job.py` -> Dimensión de aeropuertos.
-   `gold_build_dim_prefixes_job.py` -> Dimensión de prefijos
    (aerolíneas).
-   `gold_build_dim_dates_job.py` -> Dimensión de fechas (sintética).
-   `gold_build_dim_airlines_job.py` -> Dimensión de aerolíneas (derivada
    de flights).

### DAGs

-   `gold_facts.py` -> Orquesta construcción de tablas de hechos.
-   `gold_dims.py` -> Orquesta construcción de dimensiones.


## Archivos implicados en la fase Gold

    opensky-lambda-pipeline/
    │
    ├── utils/
    │   ├── gold_utils.py
    │   ├── gold_metrics.py
    │   └── init_gold_manifest.py
    │
    ├── spark_jobs/
    │   ├── gold_build_fact_flights_job.py
    │   ├── gold_build_fact_states_job.py
    │   ├── gold_build_dim_airports_job.py
    │   ├── gold_build_dim_prefixes_job.py
    │   ├── gold_build_dim_dates_job.py
    │   └── gold_build_dim_airlines_job.py
    │
    └── dags/
        ├── gold_facts.py
        └── gold_dims.py


## Inventario de datasets (Gold)

| Tabla               | Carga                   | Partición        | Clave/Upsert               | Fuente base                            |
|---------------------|-------------------------|------------------|----------------------------|----------------------------------------|
| `gold.fact_flights` | Merge/Append (por lote) | `dep_date_utc`   | `flight_id`                | `silver.flights_departure` + `arrival` |
| `gold.fact_states`  | Overwrite por ventana   | `event_date_utc` | (`icao24`,`event_date_utc`) | `silver.states`                        |
| `gold.dim_airports` | Merge/Upsert            | —                | `icao`                     | `silver.ref_airports`                  |
| `gold.dim_prefixes` | Merge/Upsert            | —                | `prefix`                   | `silver.ref_airlines_prefixes`         |
| `gold.dim_airlines` | Overwrite               | —                | —                          | `gold.fact_flights`                    |
| `gold.dim_dates`    | Overwrite               | —                | —                          | Generador                              |


> **Nota**: `coverage_pct` en `fact_states` usa un parámetro ajustable (`EXPECTED_PINGS_PER_DAY`).
## Flujo de procesamiento (resumen)

1.  **Lectura Silver** -> states/flights/ref\_\*.
2.  **Transformación/Join** -> derivadas, claves de negocio,
    enriquecimiento.
3.  **Construcción dimensiones** -> fechas, aeropuertos, prefijos,
    aerolíneas.
4.  **Construcción hechos** -> flights y states.
5.  **Escritura Delta** -> partición por fechas en hechos, snapshot en
    dimensiones.
6.  **Registro en manifiesto y métricas**.

# Fase 5 (Power BI / Reporting)

## Explicación lógica de la Fase 5  
Cerramos el **Medallion** exponiendo un **modelo semántico** en Power BI sobre la capa **Gold**.  
El objetivo es doble:

- **Validación de dato**: comprobar que fact/dim de Gold responden a las preguntas de negocio básicas.
- **Primer tablero operativo**: KPIs, ranking y tendencias mínimas para compartir con el equipo.

La ingesta hacia Power BI no requiere nuevas transformaciones: **exportamos snapshots** de las tablas Gold a **Parquet** y las conectamos en Power BI para construir el **modelo estrella**, las **medidas DAX** y los **visuales**.



## Ficha técnica de la fase Power BI

| Atributo            | Descripción |
|----------------------|-------------|
| **Definición**      | Publicación de un **modelo semántico** (estrella) en Power BI sobre tablas **Gold** exportadas a Parquet. |
| **Objetivo**        | Dashboard de validación y primeros KPIs, manteniendo trazabilidad completa al linaje Silver/Bronze/Raw. |
| **Tipo de objetos** | Parquet (snapshot), modelo Power BI (relaciones + DAX), PBIX. |
| **Método de carga** | **Snapshot** `overwrite` (opcional `coalesce(1)`) vía `gold_export_pbi_job.py`. |
| **Transformaciones**| Ninguna adicional: solo **medidas DAX** en Power BI. |
| **Audiencia**       | Data/Analytics Engineers y usuarios de negocio para exploración inicial. |




## Componentes principales

- **Exportador a BI**  
  `spark_jobs/gold_export_pbi_job.py` -> lee Delta **Gold** y escribe **Parquet** para Power BI.
  
  ```bash
  spark-submit ... spark_jobs/gold_export_pbi_job.py     --gold-root /opt/spark/lakehouse/gold     --out-root  /opt/spark/lakehouse/pbi_exports     --tables fact_flights fact_states dim_airports dim_airlines dim_dates dim_airlines_prefixes     --single-file
  ```

- **Modelo Power BI**  
  - Conexión **Parquet** a `/lakehouse/pbi_exports/<tabla>/`.
  - **Relaciones 1->*** entre dimensiones (`dim_*`) y hechos (`fact_*`).
  - Tabla técnica **`_Measures`** con DAX.



## Archivos implicados en la fase Power BI

```
opensky-lambda-pipeline/
│
├── spark_jobs/
│   └── gold_export_pbi_job.py             # Export a Parquet para Power BI
│
└── lakehouse/pbi_exports/                 # Salida para PBIX
    ├── fact_flights/part-*.parquet
    ├── fact_states/part-*.parquet
    ├── dim_airports/part-*.parquet
    ├── dim_airlines/part-*.parquet
    ├── dim_airlines_prefixes/part-*.parquet
    └── dim_dates/part-*.parquet
```



## Modelo semántico (estrella) y relaciones

Relaciones **activas** (filtro en **Ambas**), cardinalidad **1->***:

- `dim_dates[date]            1 -> *  fact_flights[dep_date_utc]`
- `dim_dates[date]            1 -> *  fact_states[event_date_utc]`
- `dim_airports[icao]         1 -> *  fact_flights[dep_icao]`
- `dim_airports[icao]         1 -> *  fact_flights[arr_icao]` *(recomendada **inactiva** y activar por DAX cuando aplique con `USERELATIONSHIP`)*
- `dim_airlines[airline_name] 1 -> *  fact_flights[airline_name]`
- `dim_airlines_prefixes[prefix] 1 -> * fact_flights[airline_prefix]`

![Modelado](/docs/imgs/Modelos_gold.jpg)




## Medidas DAX (tabla `_Measures`)

> **Conteos y particiones**

```DAX
Flights = COUNTROWS ( fact_flights )

Flights (Arrivals) =
    CALCULATE ( [Flights], NOT ISBLANK ( fact_flights[arr_icao] ) )

Flights (Departures) =
    CALCULATE ( [Flights], NOT ISBLANK ( fact_flights[dep_icao] ) )
```

> **Block time**

```DAX
Avg Block Time (min) =
    AVERAGEX ( fact_flights, DIVIDE ( fact_flights[block_time_s], 60 ) )

Median Block Time (min) =
    PERCENTILEX.INC ( fact_flights, DIVIDE ( fact_flights[block_time_s], 60 ), 0.5 )
```

> **Percentiles y calidad (states)**

```DAX
p95 Velocity (m/s) =
    PERCENTILEX.INC ( fact_states, fact_states[p95_velocity], 0.95 )

p95 Altitude (m) =
    PERCENTILEX.INC ( fact_states, fact_states[p95_altitude], 0.95 )

Coverage % = AVERAGE ( fact_states[coverage_pct] )

Velocity Outlier % =
    AVERAGEX ( fact_states, fact_states[pct_velocity_outlier] )

Altitude Outlier % =
    AVERAGEX ( fact_states, fact_states[pct_alt_outlier] )
```

> **Unknowns**

```DAX
Unknown Dep % =
    DIVIDE (
        CALCULATE ( COUNTROWS ( fact_flights ), fact_flights[unknown_dep] = TRUE() ),
        [Flights]
    ) * 100

Unknown Arr % =
    DIVIDE (
        CALCULATE ( COUNTROWS ( fact_flights ), fact_flights[unknown_arr] = TRUE() ),
        [Flights]
    ) * 100
```

> **Relación alternativa (cuando `arr_icao` sea la clave del visual)**

```DAX
Flights by Arr Airport =
    CALCULATE ( [Flights], USERELATIONSHIP ( dim_airports[icao], fact_flights[arr_icao] ) )

Flights by Dep Airport =
    CALCULATE ( [Flights], USERELATIONSHIP ( dim_airports[icao], fact_flights[dep_icao] ) )
```



## Visualizaciones seleccionadas (dashboard de validación)

- **Indicadores generales** (tarjetas)
  - `Flights`, `Coverage %`, `Avg Block Time (min)`,
    `p95 Altitude (m)`, `p95 Velocity (m/s)`, `Unknown Arr %`

- **Top Aeropuertos**  
  Barras horizontales agrupadas con `Flights (Departures)` y `Flights (Arrivals)`; eje `dim_airports[name]`.  
  Filtro Top-N para legibilidad.

- **Vuelos / Día**  
  Líneas con `Flights (Departures)` y `Flights (Arrivals)`; eje `dim_dates[date]`.

- **Top Aerolíneas**  
  Barras horizontales con `Flights` por `dim_airlines[airline_name]` (Top-N).

> **Reporting (captura)**  
> _Sustituir por tu imagen_ -> `docs/img/pbi_reporting_overview.png`

![Dashboard Validacion](/docs/imgs/Dashboard.jpg)



## Operación & troubleshooting

- **Export “.parquet” único**: usar `--single-file` en entornos locales; en servicio/OneLake, preferible particionado.  
- **Relaciones dobles aeropuerto**: mantener una **activa** (p.ej., `dep_icao`) y activar la de `arr_icao`
  con `USERELATIONSHIP` en medidas específicas.  
- **Un solo punto en dispersión**: añadir **Detalle** (p.ej., `airline_name`) para romper la agregación.  
- **Corrector ortográfico** en cuadros de texto: desactivar en *Archivo -> Opciones -> Global -> Corrección ortográfica*.



## Próximos pasos

- Segmentadores (fechas, aeropuertos, aerolíneas) y **drill-through**.  
- **Mapa** de aeropuertos (añadiendo lat/long a `dim_airports`).  
- Publicación en **Power BI Service** y refresh coordinado tras los DAGs Gold.  
- Medidas adicionales (p25/p75/p95 de block time, turnaround v2, puntualidad si se añade fuente de horarios).



> Con esta fase queda **exhibido el Medallion completo**: desde **Raw -> Bronze -> Silver -> Gold -> Power BI**, con linaje, control de cambios e idempotencia, y un **primer tablero operativo** listo para iterar con negocio.

