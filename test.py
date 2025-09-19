#!/usr/bin/env python3
import os
import sys
import json
import time
from datetime import datetime, timezone
import argparse
import requests

API_URL = "https://opensky-network.org/api/tracks/all"

def iso(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()

def fetch_track(icao24: str, t: int = 0, timeout: int = 20) -> dict | None:
    """
    Pide /tracks/all para un icao24 (minúsculas).
    t=0 -> live; si no, un timestamp UNIX dentro del vuelo.
    Soporta Basic Auth vía variables de entorno OSKY_USER/OSKY_PASS (opcional).
    """
    auth = None
    user = os.getenv("OSKY_USER")
    pwd = os.getenv("OSKY_PASS")
    if user and pwd:
        auth = (user, pwd)

    params = {"icao24": icao24.lower(), "time": t}
    headers = {"User-Agent": "opensky-demo/1.0"}
    r = requests.get(API_URL, params=params, headers=headers, timeout=timeout, auth=auth)
    if r.status_code == 404:
        print("[INFO] Sin track para ese instante (404). Prueba con otro 'time' o verifica que hay vuelo activo.")
        return None
    r.raise_for_status()
    return r.json()

def print_track(payload: dict) -> None:
    icao = payload.get("icao24")
    callsign = (payload.get("callsign") or "").strip() or "-"
    start_ts = payload.get("startTime")
    end_ts = payload.get("endTime")
    path = payload.get("path") or []

    print("\n=== RESUMEN TRACK ===")
    print(f"icao24      : {icao}")
    print(f"callsign    : {callsign}")
    print(f"startTime   : {start_ts}  ({iso(start_ts)})")
    print(f"endTime     : {end_ts}    ({iso(end_ts)})")
    print(f"waypoints   : {len(path)}")

    def fmt_pt(pt):
        # [time, lat, lon, baro_altitude, true_track, on_ground]
        t, lat, lon, alt, hdg, on_gnd = (pt + [None]*6)[:6]
        return {
            "time": t, "time_iso": iso(t),
            "lat": lat, "lon": lon,
            "alt_m": alt, "hdg_deg": hdg,
            "on_ground": on_gnd
        }

    if path:
        first = path[:5]
        last = path[-5:] if len(path) > 5 else []
        print("\n--- Primeros puntos ---")
        for i, pt in enumerate(first, 1):
            print(json.dumps(fmt_pt(pt), ensure_ascii=False))
        if last:
            print("\n--- Últimos puntos ---")
            for i, pt in enumerate(last, 1):
                print(json.dumps(fmt_pt(pt), ensure_ascii=False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch OpenSky track for a given icao24.")
    parser.add_argument("--icao24", default="39de4c", help="ICAO24 en hex (minúsculas).")
    parser.add_argument("--time", type=int, default=0, help="0 = live; o UNIX timestamp dentro del vuelo.")
    args = parser.parse_args()

    try:
        payload = fetch_track(args.icao24, args.time)
        if payload:
            print_track(payload)
    except requests.HTTPError as e:
        print(f"[HTTP ERROR] {e.response.status_code}: {e.response.text[:300]}")
        sys.exit(1)
    except requests.RequestException as e:
        print(f"[REQUEST ERROR] {e}")
        sys.exit(1)
