import argparse
import uuid
from pyspark.sql import functions as F, types as T

from spark_session import get_spark_session
from bronze_utils import add_common_metadata, write_delta
from init_bronze_manifest import ensure_manifest, discover_new_files, update_manifest
from bronze_metrics import (
    ensure_bronze_metrics_table,
    capture_metrics,
    write_metrics,
)

# --------- ESQUEMA explícito para NDJSON de flights_arrival ----------
SCHEMA_FLIGHTS_DEPARTURE = T.StructType([
    T.StructField("icao24", T.StringType(),  True),
    T.StructField("firstSeen", T.LongType(),    True),
    T.StructField("estDepartureAirport", T.StringType(),  True),
    T.StructField("lastSeen", T.LongType(),    True),
    T.StructField("estArrivalAirport", T.StringType(),  True),
    T.StructField("callsign", T.StringType(),  True),
    T.StructField("estDepartureAirportHorizDistance", T.IntegerType(), True),
    T.StructField("estDepartureAirportVertDistance", T.IntegerType(), True),
    T.StructField("estArrivalAirportHorizDistance", T.IntegerType(), True),
    T.StructField("estArrivalAirportVertDistance", T.IntegerType(), True),
    T.StructField("departureAirportCandidatesCount", T.IntegerType(), True),
    T.StructField("arrivalAirportCandidatesCount", T.IntegerType(), True),
])

def maybe_register_table(spark, output_path: str, db: str = "bronze", table: str = "flights_departure") -> None:
    """Registra la tabla en el metastore si no existe (USING delta LOCATION ...)"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.{table}
        USING delta
        LOCATION '{output_path}'
    """)

def main(input_root: str, output_path: str, max_files: int | None, register_table: bool) -> None:
    spark = get_spark_session("Bronze Flights Departure Ingest")

    dataset = "flights_departure"
    ingest_id = str(uuid.uuid4())

    # 1) Manifiesto + tabla de métricas
    ensure_manifest(spark)
    ensure_bronze_metrics_table(spark)

    # 2) Descubrir RAW nuevos
    files = discover_new_files(spark, input_root, dataset, pattern="tag=*/date=*/*.json.gz")
    if max_files:
        files = files[:max_files]

    if not files:
        print("[OK] No hay ficheros nuevos que procesar (departure).")
        spark.stop()
        return

    print(f"[INFO] Departure a procesar ({len(files)}). Ejemplo: {files[:3]}")
    # 3) Leer NDJSON con esquema (modo PERMISSIVE para no reventar por sucios)
    df_raw = (
        spark.read
             .schema(SCHEMA_FLIGHTS_DEPARTURE)
             .option("mode", "PERMISSIVE")
             .option("columnNameOfCorruptRecord", "_corrupt_record")
             .json(files)
    )

    # 4) Metadatos comunes + extraer "tag" del path + métricas
    df_tagged = df_raw.withColumn(
    "tag", F.upper(F.regexp_extract(F.input_file_name(), r"/tag=([^/]+)/", 1))
    )

    df_bronze = add_common_metadata(df_tagged, ingest_id=ingest_id)
    batch_key = f"batch://{dataset}/{ingest_id}"  # clave de lote simple
    META_COLS = {"_ingest_id","_ingest_ts","_source_file","_ingestion_date","download_ts","download_date","_corrupt_record"}
    rows = df_bronze.count()
    metrics = capture_metrics(
        df_bronze,
        dataset=dataset,
        source_file=batch_key,
        ingest_id=ingest_id,
        exclude_cols=META_COLS,   # null_count más útil sin metadatos
        rows_in=rows
    )


    # 5) Escribir Delta (partición por fecha de descarga)
    write_delta(df_bronze, dest=output_path, partition_cols=["download_date"])
    write_metrics(spark, metrics, rows_out=rows, status="ok")

    # 6) Registrar tabla (opcional)
    if register_table:
        maybe_register_table(spark, output_path, db="bronze", table="flights_departure")

    # 7) Manifiesto -> done
    update_manifest(spark, files, dataset, ingest_id)

    print(f"[OK] flights_departure: Procesados {len(files)} ficheros -> {output_path} (Delta) + manifiesto actualizado. ingest_id={ingest_id}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta BRONZE de flights_departure (RAW NDJSON -> Delta)")
    parser.add_argument("--input", required=True, help="Raíz RAW (p.ej. /opt/spark/raw-data/flights_departure)")
    parser.add_argument("--output", required=True, help="Destino BRONZE (p.ej. /opt/spark/lakehouse/bronze/flights_departure)")
    parser.add_argument("--max-files", type=int, default=None, help="(Opcional) límite de ficheros por ejecución")
    parser.add_argument("--register-table", action="store_true", help="Registrar bronze.flights_departure en el metastore si no existe")
    args = parser.parse_args()
    main(args.input, args.output, args.max_files, args.register_table)