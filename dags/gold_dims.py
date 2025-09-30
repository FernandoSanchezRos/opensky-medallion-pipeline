from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

LAKE = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
DELTA_PKG = os.getenv("DELTA_PKG", "io.delta:delta-spark_2.12:3.2.0")
IVY = os.getenv("SPARK_IVY", "/opt/spark/.ivy2")
GOLD_VERSION = os.getenv("GOLD_VERSION", "v1.0.0")

default_args = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gold_dims",
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 3 * * *",  # diario 03:00
    catchup=False,
    default_args=default_args,
    tags=["gold","dims"],
) as dag:

    env = {"GOLD_VERSION": GOLD_VERSION, "SPARK_LAKEHOUSE": LAKE}

    dim_airports = SparkSubmitOperator(
        task_id="dim_airports",
        application="/opt/spark/spark_jobs/gold/dims/build_dim_airports.py",
        name="gold_dim_airports",
        conf={"spark.jars.ivy": IVY},
        packages=DELTA_PKG,
        application_args=["--register-table", "--gold-run-id", "airflow"],
        conn_id=None,
        verbose=False,
        driver_memory="2g",
        executor_memory="2g",
        executor_cores=1,
        num_executors=2,
        env_vars=env,
        master=SPARK_MASTER,
        deploy_mode="client",
    )

    dim_prefixes = SparkSubmitOperator(
        task_id="dim_prefixes",
        application="/opt/spark/spark_jobs/gold/dims/build_dim_prefixes.py",
        name="gold_dim_prefixes",
        conf={"spark.jars.ivy": IVY},
        packages=DELTA_PKG,
        application_args=["--register-table", "--gold-run-id", "airflow"],
        env_vars=env, master=SPARK_MASTER, deploy_mode="client",
        driver_memory="2g", executor_memory="2g", executor_cores=1, num_executors=2,
    )

    dim_dates = SparkSubmitOperator(
        task_id="dim_dates",
        application="/opt/spark/spark_jobs/gold/dims/build_dim_dates.py",
        name="gold_dim_dates",
        conf={"spark.jars.ivy": IVY},
        packages=DELTA_PKG,
        application_args=[
            "--register-table", "--gold-run-id", "airflow",
            "--years-back", "5", "--years-fwd", "2"
        ],
        env_vars=env, master=SPARK_MASTER, deploy_mode="client",
        driver_memory="2g", executor_memory="2g", executor_cores=1, num_executors=2,
    )

    dim_airlines = SparkSubmitOperator(
        task_id="dim_airlines",
        application="/opt/spark/spark_jobs/gold/dims/build_dim_airlines.py",
        name="gold_dim_airlines",
        conf={"spark.jars.ivy": IVY},
        packages=DELTA_PKG,
        application_args=["--register-table", "--gold-run-id", "airflow"],
        env_vars=env, master=SPARK_MASTER, deploy_mode="client",
        driver_memory="2g", executor_memory="2g", executor_cores=1, num_executors=2,
    )

    # Orden: airports/prefixes -> airlines (opcional), y dates independiente
    [dim_airports, dim_prefixes] >> dim_airlines
    # dim_dates no depende de nada
