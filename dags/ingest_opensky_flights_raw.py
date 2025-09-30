from __future__ import annotations

from airflow.decorators import task
from airflow import DAG
from datetime import timedelta
import pendulum
import time
import requests

from opensky_client import OpenSkyClient
from airports import load_airports

AIRPORTS_PATH = "/opt/airflow/raw-data/airports/peninsular_airports.csv"

START_DATE = pendulum.now("Europe/Madrid") - timedelta(hours=12)

with DAG(
    dag_id="opensky_flights_raw",
    start_date=START_DATE,
    schedule=None,#timedelta(hours=12),
    catchup=False,
    max_active_runs=1,
    tags=["flights", "raw"],
) as dag:

    @task
    def load_airports_task() -> list[str]:
        """
        Lee ICAOs con tu helper y limita el número de aeropuertos.
        Prioriza algunos grandes y recorta a MAX_AIRPORTS (por defecto 12).
        """
        import os

        # Máximo de aeropuertos (por defecto 12)
        MAX_AIRPORTS = int(os.getenv("MAX_AIRPORTS", "12"))

        # Lista base desde el CSV
        icaos = load_airports(AIRPORTS_PATH, col="icao")
        icaos = [c.strip().upper() for c in icaos if c]
        icaos = sorted(dict.fromkeys(icaos))  # de-dup + orden alfabético

        # Prioriza grandes (si están en el CSV)
        top_priority = [
            "LEMD","LEBL","LEMG","LEVC","LEBB","LEZL","LEAL","LEST","LECO",
            "LEXJ","LEVX","LEAS","LEJR","LEZG","LESO","LERS","LEGR",
        ]
        order = {icao: i for i, icao in enumerate(top_priority)}
        icaos.sort(key=lambda x: (x not in order, order.get(x, 999), x))

        # Limitar a MAX_AIRPORTS
        if MAX_AIRPORTS > 0:
            icaos = icaos[:MAX_AIRPORTS]

        print(f"[INFO] Seleccionados {len(icaos)} aeropuertos (MAX_AIRPORTS={MAX_AIRPORTS}). Ej: {icaos[:10]}")
        return icaos

    @task
    def fetch_flights_sequential(icaos: list[str]) -> list[dict]:
        """
        Procesa todos los aeropuertos de forma SECUENCIAL.
        - Ventana: AYER (UTC)
        - 404 => n=0 (no falla)
        - 429 => respeta Retry-After / X-Rate-Limit-Retry-After-Seconds, reintenta;
                 si agota reintentos, n=0 para no tumbar el DAG.
        - Espaciado entre peticiones para evitar ráfagas.
        """
        y = pendulum.now("UTC").subtract(days=1)
        begin = y.start_of("day").int_timestamp
        end   = y.end_of("day").add(seconds=1).int_timestamp

        client = OpenSkyClient()

        results: list[dict] = []
        per_call_sleep = 0.8   # separa las llamadas aunque no haya 429
        max_attempts = 8
        base_sleep = 1.0
        backoff = 2.0

        def call_with_policy(kind: str, icao: str):
            for attempt in range(1, max_attempts + 1):
                try:
                    if kind == "arrival":
                        return client.fetch_flights_arrival(icao, begin, end)
                    else:
                        return client.fetch_flights_departure(icao, begin, end)
                except requests.HTTPError as e:
                    resp = e.response
                    code = resp.status_code if resp is not None else None

                    # 404 -> sin datos: devolver 0
                    if code == 404:
                        path = client._flights_path(kind, icao)
                        print(f"[INFO] {kind.upper()} {icao} 404 (sin datos) -> n=0")
                        return str(path), 0

                    # 429 -> lee Retry-After y reintenta
                    if code == 429:
                        ra_hdr = (resp.headers.get("X-Rate-Limit-Retry-After-Seconds")
                                  or resp.headers.get("Retry-After"))
                        try:
                            sleep_s = float(ra_hdr) if ra_hdr is not None else base_sleep * (backoff ** (attempt - 1))
                        except Exception:
                            sleep_s = base_sleep * (backoff ** (attempt - 1))
                        remaining = resp.headers.get("X-Rate-Limit-Remaining")
                        print(f"[WARN] {kind.upper()} {icao} 429 (rate limit). "
                              f"retry_after={ra_hdr} remaining={remaining} -> sleep {sleep_s:.1f}s "
                              f"(intento {attempt}/{max_attempts})")
                        time.sleep(sleep_s)
                        continue

                    # Otros códigos: relanzar
                    raise

            # agotados reintentos de 429 -> no fallar el DAG
            path = client._flights_path(kind, icao)
            print(f"[WARN] {kind.upper()} {icao} 429 persistente tras {max_attempts} intentos => n=0")
            return str(path), 0

        for idx, icao in enumerate(icaos, start=1):
            # arrivals
            path_a, n_a = call_with_policy("arrival", icao)
            print(f"[OK] arrivals {icao} = {n_a} -> {path_a}")

            # separador suave entre peticiones
            time.sleep(per_call_sleep)

            # departures
            path_d, n_d = call_with_policy("departure", icao)
            print(f"[OK] departures {icao} = {n_d} -> {path_d}")

            results.append({"airport": icao, "arrivals": n_a, "departures": n_d})

            # separador entre aeropuertos
            time.sleep(per_call_sleep)

            # log de progreso
            if idx % 5 == 0:
                print(f"[INFO] Progreso: {idx}/{len(icaos)} aeropuertos")

        return results

    icaos = load_airports_task()
    fetch_flights_sequential(icaos)
