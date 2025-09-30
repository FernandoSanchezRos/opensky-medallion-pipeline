# spark_jobs/bronze_static_job.py
import argparse, uuid, os
from pyspark.sql import types as T
from spark_session import get_spark_session
from bronze_utils import add_common_metadata, write_delta
from init_bronze_manifest import (
    ensure_manifest, snapshot_needs_processing, update_manifest, compute_file_hash
)
from bronze_metrics import (
    ensure_bronze_metrics_table,
    capture_metrics,
    write_metrics,
)

# --- Esquemas tal cual tus CSV ---
SCHEMA_AIRLINES_PREFIXES = T.StructType([
    T.StructField("prefix",  T.StringType(), True),
    T.StructField("airline", T.StringType(), True),
])

SCHEMA_AIRPORTS = T.StructType([
    T.StructField("city",   T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("icao",   T.StringType(), True),
    T.StructField("iata",   T.StringType(), True),
    T.StructField("name",   T.StringType(), True),
    T.StructField("type",   T.StringType(), True),
])

SCHEMAS = {
    "airlines_prefixes": SCHEMA_AIRLINES_PREFIXES,
    "airports":          SCHEMA_AIRPORTS,
}

def maybe_register_table(spark, output_path: str, db: str, table: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def main(dataset: str, input_file: str, output_path: str, register_table: bool) -> None:
    spark = get_spark_session(f"Bronze Static: {dataset}")
    ensure_manifest(spark)
    ensure_bronze_metrics_table(spark)

    if not os.path.exists(input_file):
        raise FileNotFoundError(input_file)

    meta_dataset = f"ref_{dataset}"

    # Skip si el fichero no cambió
    if not snapshot_needs_processing(spark, dataset=meta_dataset, file_path=input_file):
        print(f"[OK] {meta_dataset}: snapshot sin cambios. Nada que hacer.")
        spark.stop()
        return

    ingest_id = str(uuid.uuid4())
    file_hash = compute_file_hash(input_file)

    # 1) Leer CSV (sin limpiezas; lo dejas para Silver)
    df_raw = (
        spark.read
             .option("header", "true")
             .schema(SCHEMAS[dataset])
             .csv(input_file)
    )

    # 2) Añadir SOLO metadatos comunes de Bronze
    df_bronze = add_common_metadata(df_raw, ingest_id=ingest_id)
    META_COLS = {"_ingest_id","_ingest_ts","_source_file","_ingestion_date","download_ts","download_date"}
    rows = df_bronze.count()
    metrics = capture_metrics(
        df_bronze,
        dataset=meta_dataset,
        source_file=input_file,
        ingest_id=ingest_id,
        exclude_cols=META_COLS,   # null_count más útil sin metadatos
        rows_in=rows  
    )

    # 3) Escribir snapshot a Delta (overwrite)
    write_delta(
        df_bronze,
        dest=output_path,
        partition_cols=[],      # sin particionar estas dims
        mode="overwrite",
        merge_schema=True,
    )
    status = "ok" if metrics["corrupt_rows"] == 0 else "warn"
    write_metrics(spark, metrics, rows_out=rows, status=status)

    if register_table:
        maybe_register_table(spark, output_path, db="bronze", table=meta_dataset)

    # 4) Marcar en manifiesto con hash (idempotencia/auditoría)
    update_manifest(
        spark,
        files=[input_file],
        dataset=meta_dataset,
        ingest_id=ingest_id,
        status="done",
        file_hash=file_hash,
    )

    print(f"[OK] {meta_dataset}: escrito en {output_path} (Delta). ingest_id={ingest_id}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Ingesta BRONZE estáticos (snapshot con hash, sin limpiezas)")
    p.add_argument("--dataset", required=True, choices=["airlines_prefixes", "airports"])
    p.add_argument("--input-file", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--register-table", action="store_true")
    args = p.parse_args()
    main(args.dataset, args.input_file, args.output, args.register_table)
