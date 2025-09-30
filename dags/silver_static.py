# dags/silver_ref.py
from __future__ import annotations

import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# --- Helpers ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

def resolve(path: str) -> str:
    return path if os.path.isabs(path) else os.path.abspath(os.path.join(BASE_DIR, path))

# --- Config desde .env (igual que Bronze) ---
SPARK_IMAGE    = os.getenv("SPARK_IMAGE", "custom-spark:3.5")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "opensky-lambda-pipeline_default")

HOST_RAW   = resolve(os.getenv("HOST_RAW", "./raw-data"))
HOST_LAKE  = resolve(os.getenv("HOST_LAKEHOUSE", "./lakehouse"))
HOST_JOBS  = resolve(os.getenv("HOST_SPARK_JOBS", "./spark_jobs"))
HOST_UTILS = resolve(os.getenv("HOST_UTILS", "./utils"))

CT_RAW   = os.getenv("SPARK_RAW", "/opt/spark/raw-data")
CT_LAKE  = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
CT_JOBS  = os.getenv("SPARK_JOBS_ROOT", "/opt/spark/spark_jobs")
CT_UTILS = os.getenv("SPARK_UTILS", "/opt/spark/utils")

# Versión lógica de Silver (opcional)
SILVER_VERSION = os.getenv("SILVER_VERSION", "v1.0.0")

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
    dag_id="silver_ref",
    description="Transformación Bronze → Silver de estáticos (airlines_prefixes, airports)",
    start_date=pendulum.datetime(2025, 9, 25, 0, 0, tz="Europe/Madrid"),
    schedule=None, #timedelta(weeks=1),  # igual que bronze_ref
    catchup=False,
    max_active_runs=1,
    tags=["silver", "ref"],
) as dag:

    # --- Airlines Prefixes ---
    cmd_airlines = (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--conf spark.jars.ivy=/opt/spark/.ivy2 "
        "--packages io.delta:delta-spark_2.12:3.2.0 "
        f"{CT_JOBS}/silver_static_job.py "
        "--dataset airlines_prefixes "
        f"--bronze-input {CT_LAKE}/bronze/ref_airlines_prefixes "
        f"--silver-output {CT_LAKE}/silver/ref_airlines_prefixes "
        f"--silver-version {SILVER_VERSION} "
        "--register-table "
    )

    silver_airlines_prefixes = DockerOperator(
        task_id="silver_airlines_prefixes_job",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        user="50000:0",
        command=cmd_airlines,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        environment={
            "PYSPARK_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYTHONPATH": f"/opt/spark:{CT_JOBS}:{CT_UTILS}",
            "SPARK_LAKEHOUSE": CT_LAKE,
            "SILVER_VERSION": SILVER_VERSION,
        },
    )

    # --- Airports ---
    cmd_airports = (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--deploy-mode client "
        "--conf spark.jars.ivy=/opt/spark/.ivy2 "
        "--packages io.delta:delta-spark_2.12:3.2.0 "
        f"{CT_JOBS}/silver_static_job.py "
        "--dataset airports "
        f"--bronze-input {CT_LAKE}/bronze/ref_airports "
        f"--silver-output {CT_LAKE}/silver/ref_airports "
        f"--silver-version {SILVER_VERSION} "
        "--register-table "
    )

    silver_airports = DockerOperator(
        task_id="silver_airports_job",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        user="50000:0",
        command=cmd_airports,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        environment={
            "PYSPARK_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/opt/bitnami/python/bin/python3",
            "PYTHONPATH": f"/opt/spark:{CT_JOBS}:{CT_UTILS}",
            "SPARK_LAKEHOUSE": CT_LAKE,
            "SILVER_VERSION": SILVER_VERSION,
        },
    )

    silver_airlines_prefixes >> silver_airports
