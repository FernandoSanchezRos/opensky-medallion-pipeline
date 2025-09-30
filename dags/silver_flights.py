from __future__ import annotations

import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# --- Helpers ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # raíz del repo


def resolve(path: str) -> str:
    """Convierte ruta relativa a absoluta (basada en BASE_DIR)."""
    return path if os.path.isabs(path) else os.path.abspath(os.path.join(BASE_DIR, path))


# --- Config desde .env (mismos nombres que en bronze) ---
SPARK_IMAGE = os.getenv("SPARK_IMAGE", "custom-spark:3.5")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "opensky-lambda-pipeline_default")

HOST_RAW   = resolve(os.getenv("HOST_RAW", "./raw-data"))
HOST_LAKE  = resolve(os.getenv("HOST_LAKEHOUSE", "./lakehouse"))
HOST_JOBS  = resolve(os.getenv("HOST_SPARK_JOBS", "./spark_jobs"))
HOST_UTILS = resolve(os.getenv("HOST_UTILS", "./utils"))

CT_RAW  = os.getenv("SPARK_RAW", "/opt/spark/raw-data")
CT_LAKE = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
CT_JOBS = os.getenv("SPARK_JOBS_ROOT", "/opt/spark/spark_jobs")
CT_UTILS= os.getenv("SPARK_UTILS", "/opt/spark/utils")

# --- Opcionales / tuning ---
SILVER_VERSION = os.getenv("SILVER_VERSION", "v1.0.0")
SILVER_FLIGHTS_PATTERN = os.getenv("SILVER_FLIGHTS_PATTERN", "download_date=*/*.parquet")
SILVER_FLIGHTS_MAX_FILES = os.getenv("SILVER_FLIGHTS_MAX_FILES", "")  # vacío -> sin límite

# Rutas de referencias (usamos SILVER para enriquecer con datos ya limpios)
REF_AIRLINES = os.getenv("REF_AIRLINES_SILVER", f"{CT_LAKE}/silver/ref_airlines_prefixes")
REF_AIRPORTS = os.getenv("REF_AIRPORTS_SILVER", f"{CT_LAKE}/silver/ref_airports")

# --- Montajes ---
mounts = [
    Mount(source=HOST_RAW,   target=CT_RAW,  type="bind", read_only=False),
    Mount(source=HOST_LAKE,  target=CT_LAKE, type="bind", read_only=False),
    Mount(source=HOST_JOBS,  target=CT_JOBS, type="bind", read_only=False),
    Mount(source=HOST_UTILS, target=CT_UTILS,type="bind", read_only=False),
    Mount(source="ivy_cache", target="/opt/spark/.ivy2", type="volume"),
]

# --- DAG ---
with DAG(
    dag_id="silver_flights",
    description="Transformación Bronze → Silver de flights (arrivals & departures) en Delta Lake",
    start_date=pendulum.datetime(2025, 9, 25, 0, 0, tz="Europe/Madrid"),
    schedule=None,#timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "flights"],
) as dag:

    # --------- Arrivals -------------
    cmd_arrivals = (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--conf spark.jars.ivy=/opt/spark/.ivy2 "
        "--packages io.delta:delta-spark_2.12:3.2.0 "
        f"{CT_JOBS}/silver_flights_arrival_job.py "
        f"--input {CT_LAKE}/bronze/flights_arrival "
        f"--output {CT_LAKE}/silver/flights_arrival "
        f"--ref-airlines {REF_AIRLINES} "
        f"--ref-airports {REF_AIRPORTS} "
        f"--pattern '{SILVER_FLIGHTS_PATTERN}' "
        f"--silver-version {SILVER_VERSION} "
        "--register-table "
    )
    if SILVER_FLIGHTS_MAX_FILES.strip():
        cmd_arrivals += f"--max-files {SILVER_FLIGHTS_MAX_FILES.strip()} "

    run_silver_flights_arrival = DockerOperator(
        task_id="silver_flights_arrival_job",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        user="50000:0",
        command=cmd_arrivals,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        tty=False,
        environment={
            "PYSPARK_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYTHONPATH": f"/opt/spark:{CT_JOBS}:{CT_UTILS}",
            # Defaults que usan los jobs si no se pasan por CLI:
            "SPARK_LAKEHOUSE": CT_LAKE,
            "SILVER_VERSION": SILVER_VERSION,
        },
    )

    # --------- Departures -------------
    cmd_departures = (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--conf spark.jars.ivy=/opt/spark/.ivy2 "
        "--packages io.delta:delta-spark_2.12:3.2.0 "
        f"{CT_JOBS}/silver_flights_departure_job.py "
        f"--input {CT_LAKE}/bronze/flights_departure "
        f"--output {CT_LAKE}/silver/flights_departure "
        f"--ref-airlines {REF_AIRLINES} "
        f"--ref-airports {REF_AIRPORTS} "
        f"--pattern '{SILVER_FLIGHTS_PATTERN}' "
        f"--silver-version {SILVER_VERSION} "
        "--register-table "
    )
    if SILVER_FLIGHTS_MAX_FILES.strip():
        cmd_departures += f"--max-files {SILVER_FLIGHTS_MAX_FILES.strip()} "

    run_silver_flights_departure = DockerOperator(
        task_id="silver_flights_departure_job",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        user="50000:0",
        command=cmd_departures,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        tty=False,
        environment={
            "PYSPARK_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYTHONPATH": f"/opt/spark:{CT_JOBS}:{CT_UTILS}",
            "SPARK_LAKEHOUSE": CT_LAKE,
            "SILVER_VERSION": SILVER_VERSION,
        },
    )

    # Orden igual que en bronze: primero arrivals, luego departures (pueden ir en paralelo si lo prefieres)
    run_silver_flights_arrival >> run_silver_flights_departure
