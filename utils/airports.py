from pathlib import Path
import csv

def load_airports(csv_path: str | Path, col: str = "ICAO") -> list[str]:
    """
    Lee un CSV de aeropuertos y devuelve la lista de ICAOs (u otra columna).
    Por defecto, devuelve ICAO.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row[col].strip() for row in reader if row.get(col)]