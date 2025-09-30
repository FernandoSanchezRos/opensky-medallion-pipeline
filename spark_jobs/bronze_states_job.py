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

# --------- ESQUEMA explícito para NDJSON de states ----------
SCHEMA_STATES = T.StructType([
    T.StructField("icao24",          T.StringType(),  True),
    T.StructField("callsign",        T.StringType(),  True),
    T.StructField("origin_country",  T.StringType(),  True),
    T.StructField("time_position",   T.LongType(),    True),
    T.StructField("last_contact",    T.LongType(),    True),
    T.StructField("longitude",       T.DoubleType(),  True),
    T.StructField("latitude",        T.DoubleType(),  True),
    T.StructField("baro_altitude",   T.DoubleType(),  True),
    T.StructField("on_ground",       T.BooleanType(), True),
    T.StructField("velocity",        T.DoubleType(),  True),
    T.StructField("true_track",      T.DoubleType(),  True),
    T.StructField("vertical_rate",   T.DoubleType(),  True),
    T.StructField("sensors",         T.ArrayType(T.IntegerType()), True),
    T.StructField("geo_altitude",    T.DoubleType(),  True),
    T.StructField("squawk",          T.StringType(),  True),
    T.StructField("spi",             T.BooleanType(), True),
    T.StructField("position_source", T.IntegerType(), True),
    T.StructField("category",        T.IntegerType(), True),
])

def maybe_register_table(spark, output_path: str, db: str = "bronze", table: str = "states") -> None:
    """Registra la tabla en el metastore si no existe (USING delta LOCATION ...)"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.{table}
        USING delta
        LOCATION '{output_path}'
    """)

def main(input_root: str, output_path: str, max_files: int | None, register_table: bool) -> None:
    spark = get_spark_session("Bronze States Ingest")

    dataset = "states"
    ingest_id = str(uuid.uuid4())

    # 1) Manifiesto + tabla de métricas
    ensure_manifest(spark)
    ensure_bronze_metrics_table(spark)

    # 2) Descubrir RAW nuevos
    files = discover_new_files(spark, input_root, dataset)
    if max_files:
        files = files[:max_files]

    if not files:
        print("[OK] No hay ficheros nuevos que procesar.")
        spark.stop()
        return

    print(f"[INFO] Ficheros a procesar ({len(files)}). Ejemplo: {files[:3]}")

    # 3) Leer NDJSON con esquema (modo PERMISSIVE para no reventar por sucios)
    df_raw = (
        spark.read
             .schema(SCHEMA_STATES)
             .option("mode", "PERMISSIVE")
             .option("columnNameOfCorruptRecord", "_corrupt_record")
             .json(files)
    )

    # 4) Metadatos comunes y métricas
    df_bronze = add_common_metadata(df_raw, ingest_id=ingest_id)

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
        maybe_register_table(spark, output_path, db="bronze", table="states")

    # 7) Manifiesto -> done
    update_manifest(spark, files, dataset, ingest_id)

    print(f"[OK] states: Procesados {len(files)} ficheros -> {output_path} (Delta) + manifiesto actualizado. ingest_id={ingest_id}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta BRONZE de states (RAW NDJSON -> Delta)")
    parser.add_argument("--input", required=True, help="Raíz RAW (p.ej. /opt/spark/raw-data/states)")
    parser.add_argument("--output", required=True, help="Destino BRONZE (p.ej. /opt/spark/lakehouse/bronze/states)")
    parser.add_argument("--max-files", type=int, default=None, help="(Opcional) límite de ficheros por ejecución")
    parser.add_argument("--register-table", action="store_true", help="Registrar bronze.states en el metastore si no existe")
    args = parser.parse_args()
    main(args.input, args.output, args.max_files, args.register_table)