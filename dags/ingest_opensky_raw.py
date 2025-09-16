from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from opensky_client import poll_opensky
from dotenv import load_dotenv

load_dotenv()


with DAG(
    dag_id="ingest_opensky_raw",
    start_date=datetime(2023, 1, 1),
    schedule=timedelta(seconds=90),
    catchup=False,
    tags=["opensky", "ingest", "raw"]
) as dag:

    @task(task_id="run_poll_opensky")
    def ingest_opensky_raw():
        poll_opensky()

    ingest_opensky_raw()