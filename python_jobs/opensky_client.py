import os
import json
from datetime import datetime, timezone

import requests

from token_manager import TokenManager


def poll_opensky():
    # Obtener variables de entorno
    TOKEN_URL = os.getenv("TOKEN_URL")
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    STATES_API = os.getenv("STATES_API")

    if not (TOKEN_URL and CLIENT_ID and CLIENT_SECRET and STATES_API):
        raise ValueError("Faltan variables de entorno requeridas: TOKEN_URL, CLIENT_ID, CLIENT_SECRET o STATES_API")

    # Instanciar el TokenManager
    token_manager = TokenManager(TOKEN_URL, CLIENT_ID, CLIENT_SECRET)

    try:
        headers = token_manager.auth_header()
        response = requests.get(STATES_API, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Crear y asegurar el directorio 
        output_dir = os.getenv("RAW_DATA")
        os.makedirs(output_dir, exist_ok=True)
        
        # Generar nombre de archivo con timestamp
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_path = os.path.join(output_dir, f"batch_{now}.json")
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        
        print(f"Datos obtenidos: {len(data.get('states', []))} estados guardados en {file_path}")
    except Exception as e:
        print(f"Error durante la consulta: {e}")


def main():
    print("Ejecutando poller de OpenSky...")
    poll_opensky()


if __name__ == "__main__":
    main()
