# dags/bronze_states.py
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


# --- Config desde .env ---
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
    dag_id="bronze_states",
    description="Ingesta de RAW states -> Bronze (Delta Lake)",
    start_date=pendulum.datetime(2025, 9, 25, 0, 0, tz="Europe/Madrid"),
    schedule=None,#timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "states"],
) as dag:

    run_bronze_states = DockerOperator(
        task_id="bronze_states_job",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        user="50000:0",
        command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--conf spark.jars.ivy=/opt/spark/.ivy2 "
            "--packages io.delta:delta-spark_2.12:3.2.0 "
            "/opt/spark/spark_jobs/bronze_states_job.py "
            f"--input {CT_RAW}/states "
            f"--output {CT_LAKE}/bronze/states "
            "--register-table"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        tty=False,

        environment={
        "PYSPARK_PYTHON": "/opt/bitnami/python/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/opt/bitnami/python/bin/python3",
        "PYTHONPATH": f"/opt/spark:{CT_JOBS}:{CT_UTILS}",
        },
    )
