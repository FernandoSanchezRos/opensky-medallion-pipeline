from __future__ import annotations

from airflow.decorators import task
from airflow.operators.python import get_current_context
from opensky_client import OpenSkyClient
from airports import load_airports
from datetime import datetime, timedelta
from airflow import DAG
import pendulum

WINDOW_HOURS = 12
AIRPORTS_PATH = "/opt/airflow/raw-data/airports/peninsular_airports.csv"  

# Primer run inmediato: start_date = ahora - 12h
START_DATE = pendulum.now("Europe/Madrid") - timedelta(hours=12)

with DAG(
    dag_id="opensky_flights_raw",
    start_date=START_DATE,
    schedule=timedelta(hours=12),  # cada 12 horas (cron estable)
    catchup=False,
    tags=["opensky", "flights", "raw"],
) as dag:

    @task
    def load_airports_task() -> list[str]:
        """Lee ICAOs del CSV (columna 'ICAO')."""
        return load_airports(AIRPORTS_PATH, col="ICAO")

    @task
    def fetch_flights(icao: str) -> dict:
        """Descarga arrivals + departures de un aeropuerto en la última ventana de 12h."""
        ctx = get_current_context()
        end = int(ctx["data_interval_end"].int_timestamp)
        begin = end - (WINDOW_HOURS * 3600)

        client = OpenSkyClient()
        path_a, n_a = client.fetch_flights_arrival(icao, begin, end)
        print(f"[OK] {n_a} arrivals {icao} -> {path_a}")

        path_d, n_d = client.fetch_flights_departure(icao, begin, end)
        print(f"[OK] {n_d} departures {icao} -> {path_d}")

        return {"airport": icao, "arrivals": n_a, "departures": n_d}

    # Paralelismo por aeropuerto (dynamic task mapping)
    icaos = load_airports_task()
    fetch_flights.expand(icao=icaos)