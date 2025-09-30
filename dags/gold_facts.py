from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    dag_id="gold_facts",
    start_date=datetime(2025, 9, 1),
    schedule_interval="30 3 * * *",  # después de dims (03:30)
    catchup=False,
    default_args=default_args,
    tags=["gold","facts"],
) as dag:

    # Espera a que gold_dims haya terminado hoy
    wait_dims = ExternalTaskSensor(
        task_id="wait_for_gold_dims",
        external_dag_id="gold_dims",
        external_task_id=None,     # espera a que el DAG complete
        mode="reschedule",
        poke_interval=60,
        timeout=60*60,             # 1h
    )

    env = {"GOLD_VERSION": GOLD_VERSION, "SPARK_LAKEHOUSE": LAKE}

    fact_flights = SparkSubmitOperator(
        task_id="fact_flights",
        application="/opt/spark/spark_jobs/gold/build_fact_flights.py",
        name="gold_fact_flights",
        conf={"spark.jars.ivy": IVY},
        packages=DELTA_PKG,
        application_args=["--register-table", "--gold-run-id", "airflow"],
        env_vars=env, master=SPARK_MASTER, deploy_mode="client",
        driver_memory="3g", executor_memory="3g", executor_cores=1, num_executors=3,
    )

    fact_states = SparkSubmitOperator(
        task_id="fact_states",
        application="/opt/spark/spark_jobs/gold/build_fact_states.py",
        name="gold_fact_states",
        conf={"spark.jars.ivy": IVY},
        packages=DELTA_PKG,
        application_args=[
            "--register-table", "--gold-run-id", "airflow",
            # Ajusta si cambias EXPECTED_PINGS_PER_DAY por env var en el job
        ],
        env_vars=env, master=SPARK_MASTER, deploy_mode="client",
        driver_memory="3g", executor_memory="3g", executor_cores=1, num_executors=3,
    )

    wait_dims >> [fact_flights, fact_states]
