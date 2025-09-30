from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from opensky_client import OpenSkyClient



with DAG(
    dag_id="opensky_states_raw",
    start_date=datetime(2023, 1, 1),
    schedule=None,#timedelta(seconds=90),
    catchup=False,
    tags=["states", "raw"]
) as dag:

    @task
    def fetch_states():
        client = OpenSkyClient()
        path, n = client.fetch_states(
            lamin=35.0, lamax=45.0,   
            lomin=-10.0, lomax=4.0,
            extended=1,
        )
        print(f"[OK] {n} estados guardados en {path}")
        return {"path": path, "count": n}


    fetch_states()