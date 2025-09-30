from __future__ import annotations

import os
import json
import gzip
import time

import argparse
from typing import Any, Iterable
from datetime import datetime, timezone
from pathlib import Path

import requests

from token_manager import TokenManager

# ------------------------ Utilidades de archivo ------------------------

def utcnow_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_ndjson_gz(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    """Escribe N dicts como NDJSON comprimido (gzip). Devuelve número de líneas."""
    ensure_dir(path.parent)
    count = 0
    with gzip.open(path, "wt", encoding="utf-8") as gz:
        for obj in rows:
            gz.write(json.dumps(obj, ensure_ascii=False))
            gz.write("\n")
            count += 1
    return count


def write_json_pretty(path: Path, payload: Any) -> None:
    """Escribe JSON (pretty) comprimido (gzip si .gz)."""
    ensure_dir(path.parent)
    if path.suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as gz:
            json.dump(payload, gz, ensure_ascii=False, indent=2)
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

# ------------------------ Mapeo de "states" ------------------------

# Orden oficial de /states/all (con extended=1 añade "category" al final)
STATES_FIELDS = [
    "icao24",           # 0
    "callsign",         # 1
    "origin_country",   # 2
    "time_position",    # 3 (unix)
    "last_contact",     # 4 (unix)
    "longitude",        # 5 (deg)
    "latitude",         # 6 (deg)
    "baro_altitude",    # 7 (m)
    "on_ground",        # 8 (bool)
    "velocity",         # 9 (m/s)
    "true_track",       # 10 (deg)
    "vertical_rate",    # 11 (m/s)
    "sensors",          # 12 (array|None)
    "geo_altitude",     # 13 (m)
    "squawk",           # 14
    "spi",              # 15 (bool)
    "position_source",  # 16 (0..2)
    # 17 -> category (si extended=1)
]

def state_row_to_dict(row: list[Any]) -> dict[str, Any]:
    """Convierte un 'state vector' (lista) a dict con nombres de columna."""
    out = {name: (row[i] if i < len(row) else None) for i, name in enumerate(STATES_FIELDS)}
    # Campo extra 'category' si viene extended=1
    out["category"] = row[17] if len(row) > 17 else None

    # Normalización mínima: callsign trim/upper (sin meter más DQ en raw)
    if isinstance(out.get("callsign"), str):
        out["callsign"] = out["callsign"].strip().upper() or None
    return out

# ------------------------ Cliente OpenSky ------------------------

class OpenSkyClient:
    """
    Cliente genérico para la API de OpenSky.
    - Autenticación OAuth2 (client_credentials) vía TokenManager
    - _get: wrapper de requests con headers y retry ligero
    - Métodos fetch_* por endpoint que devuelven ruta del fichero guardado y conteo
    """

    def __init__(self,
                 base_url: str | None = None,
                 raw_root: str | None = None,
                 timeout: int = 30,
                 retries: int = 2,
                 backoff: float = 1.5):
        self.base_url = base_url or os.getenv("OPENSKY_API", "https://opensky-network.org/api")
        self.raw_root = Path(raw_root or os.getenv("AIRFLOW_RAW", "/opt/airflow/raw-data"))
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

        # Token manager (oauth2)
        token_url = os.getenv("TOKEN_URL")
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")
        self.tm = TokenManager(token_url, client_id, client_secret, timeout=max(10, timeout))

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "opensky-poller/1.0"})
        ensure_dir(self.raw_root)

    # --------------- HTTP ---------------

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = self.tm.auth_header()
        last_exc = None
        for attempt in range(self.retries + 1):
            try:
                resp = self.session.get(url, headers=headers, params=params, timeout=self.timeout)
                if resp.status_code == 401:
                    # Token expirado: forzamos refresh y reintentamos una vez más
                    _ = self.tm.get()  # refresca interno
                    headers = self.tm.auth_header()
                    resp = self.session.get(url, headers=headers, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.retries:
                    raise
                sleep_s = self.backoff ** attempt
                time.sleep(sleep_s)
        # Por si acaso
        if last_exc:
            raise last_exc

    # --------------- Paths ---------------

    def _states_path(self, scope_tag: str, api_time: int | None = None) -> Path:
        date_dir = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts = utcnow_compact()
        name = f"states_{scope_tag}_{ts}"
        if api_time is not None:
            name += f"_{api_time}"
        return self.raw_root / "states" / f"date={date_dir}" / f"{name}.json.gz"

    def _flights_path(self, kind: str, tag: str) -> Path:
        date_dir = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts = utcnow_compact()
        name = f"flights_{kind}_{tag}_{ts}.json.gz"
        return self.raw_root / f"flights_{kind}" / f"tag={tag}" / f"date={date_dir}" / name

    def _tracks_path(self, icao24: str, when_tag: str) -> Path:
        date_dir = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts = utcnow_compact()
        name = f"tracks_{icao24}_{when_tag}_{ts}.json.gz"
        return self.raw_root / "tracks" / f"icao24={icao24}" / f"date={date_dir}" / name

    # --------------- Endpoints ---------------

    def fetch_states(self,
                     lamin: float | None = None,
                     lamax: float | None = None,
                     lomin: float | None = None,
                     lomax: float | None = None,
                     extended: int = 1) -> tuple[str, int]:
        """
        Descarga /states/all (bbox opcional) y guarda como NDJSON.GZ (1 state por línea).
        Raw puro: sin metadata dentro. El nombre incluye bbox y tiempo API si está disponible.
        """
        endpoint = os.getenv("STATES_ENDPOINT", "states/all")
        params: dict[str, Any] = {"extended": extended}
        scope_tag = "global"
        if None not in (lamin, lamax, lomin, lomax):
            params.update(dict(lamin=lamin, lamax=lamax, lomin=lomin, lomax=lomax))
            # scope para nombre de archivo
            scope_tag = f"bbox_{lamin}_{lamax}_{lomin}_{lomax}".replace(".", "p")
        

        payload = self._get(endpoint=endpoint, params=params)
        states = payload.get("states") or []
        api_time = payload.get("time")

        # Convertimos array->dict y escribimos NDJSON.GZ
        def iter_rows():
            for row in states:
                # row es list; la convertimos a dict con nombres
                yield state_row_to_dict(row)

        out_path = self._states_path(scope_tag, api_time=api_time)
        count = write_ndjson_gz(out_path, iter_rows())
        return str(out_path), count

    def fetch_flights_arrival(self, airport: str, begin: int, end: int) -> tuple[str, int]:
        endpoint = os.getenv("FLIGHTS_ARRIVAL_ENDPOINT", "flights/arrival")
        payload = self._get(endpoint, params={"airport": airport, "begin": begin, "end": end})
        out_path = self._flights_path("arrival", airport)
        # Suele ser una lista de dicts: guardamos NDJSON para homogeneizar
        count = write_ndjson_gz(out_path, (row for row in payload))
        return str(out_path), count

    def fetch_flights_departure(self, airport: str, begin: int, end: int) -> tuple[str, int]:
        endpoint = os.getenv("FLIGHTS_DEPARTURE_ENDPOINT", "flights/departure")
        payload = self._get(endpoint, params={"airport": airport, "begin": begin, "end": end})
        out_path = self._flights_path("departure", airport)
        count = write_ndjson_gz(out_path, (row for row in payload))
        return str(out_path), count

    def fetch_flights_aircraft(self, icao24: str, begin: int, end: int) -> tuple[str, int]:
        endpoint = os.getenv("FLIGHTS_AIRCRAFT_ENDPOINT", "flights/aircraft")
        payload = self._get(endpoint, params={"icao24": icao24, "begin": begin, "end": end})
        out_path = self._flights_path("aircraft", icao24)
        count = write_ndjson_gz(out_path, (row for row in payload))
        return str(out_path), count

    def fetch_track(self, icao24: str, when_unix: int) -> tuple[str, int]:
        endpoint = os.getenv("TRACKS_ENDPOINT", "tracks/all")
        payload = self._get(endpoint, params={"icao24": icao24, "time": when_unix})
        # tracks es un objeto con arrays internos → para raw lo guardamos tal cual (JSON.GZ)
        tag = f"t{when_unix}"
        out_path = self._tracks_path(icao24, tag)
        write_json_pretty(out_path, payload)
        # devolvemos 1 “registro” (1 track)
        return str(out_path), 1


# ------------------------ CLI simple ------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OpenSky client (Raw capture → raw_data)")
    sub = p.add_subparsers(dest="cmd", required=True)

    # states
    ps = sub.add_parser("states", help="Fetch /states/all (optional bbox)")
    ps.add_argument("--lamin", type=float)
    ps.add_argument("--lamax", type=float)
    ps.add_argument("--lomin", type=float)
    ps.add_argument("--lomax", type=float)
    ps.add_argument("--extended", type=int, default=1)

    # arrivals
    pa = sub.add_parser("arrivals", help="Fetch /flights/arrival for an airport")
    pa.add_argument("--airport", required=True, help="ICAO code (e.g., LEMD)")
    pa.add_argument("--begin", type=int, required=True, help="unix epoch (inclusive)")
    pa.add_argument("--end", type=int, required=True, help="unix epoch (exclusive)")

    # departures
    pd = sub.add_parser("departures", help="Fetch /flights/departure for an airport")
    pd.add_argument("--airport", required=True)
    pd.add_argument("--begin", type=int, required=True)
    pd.add_argument("--end", type=int, required=True)

    # flights by aircraft
    pf = sub.add_parser("aircraft", help="Fetch /flights/aircraft for an ICAO24")
    pf.add_argument("--icao24", required=True)
    pf.add_argument("--begin", type=int, required=True)
    pf.add_argument("--end", type=int, required=True)

    # track
    pt = sub.add_parser("track", help="Fetch /tracks/all for an ICAO24 at a given time")
    pt.add_argument("--icao24", required=True)
    pt.add_argument("--time", type=int, required=True, help="unix epoch around which to fetch the track")

    return p.parse_args()


def main() -> None:
    args = _parse_args()
    client = OpenSkyClient()

    if args.cmd == "states":
        path, n = client.fetch_states(args.lamin, args.lamax, args.lomin, args.lomax, args.extended)
    elif args.cmd == "arrivals":
        path, n = client.fetch_flights_arrival(args.airport, args.begin, args.end)
    elif args.cmd == "departures":
        path, n = client.fetch_flights_departure(args.airport, args.begin, args.end)
    elif args.cmd == "aircraft":
        path, n = client.fetch_flights_aircraft(args.icao24, args.begin, args.end)
    elif args.cmd == "track":
        path, n = client.fetch_track(args.icao24, args.time)
    else:
        raise SystemExit("Comando no soportado")

    print(f"[OK] Guardado en: {path} | registros: {n}")


if __name__ == "__main__":
    main()