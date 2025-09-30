import argparse
import uuid
import os

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable

from spark_session import get_spark_session
from init_silver_manifest import ensure_manifest, discover_new_files, update_manifest
from silver_metrics import (
    ensure_silver_metrics_table, capture_metrics, write_metrics,
    with_dup_ratio, with_freshness_lag_minutes, with_extras
)
from silver_utils import add_silver_metadata  # añade _silver_version y _processing_date/_silver_ts

# --- Config por defecto via env (puedes sobreescribir con CLI) ---
LAKE_DEFAULT = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
BRONZE_STATES_DEFAULT = f"{LAKE_DEFAULT}/bronze/states"
SILVER_STATES_DEFAULT  = f"{LAKE_DEFAULT}/silver/states"
SILVER_VERSION_DEFAULT = os.getenv("SILVER_VERSION", "v1.0.0")
SILVER_RUN_ID_DEFAULT  = os.getenv("SILVER_RUN_ID", str(uuid.uuid4()))

def maybe_register_table(spark, output_path: str, db: str = "silver", table: str = "states") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def merge_upsert(spark, df_source, dest_path: str, on_expr: str, partition_cols: list[str]):
    try:
        delta = DeltaTable.forPath(spark, dest_path)
        (delta.alias("t")
              .merge(df_source.alias("s"), on_expr)
              .whenMatchedUpdateAll()
              .whenNotMatchedInsertAll()
              .execute())
    except Exception:
        writer = df_source.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(dest_path)

def main(
    bronze_path: str,
    silver_path: str,
    register_table: bool,
    max_files: int | None,
    pattern: str,
    silver_version: str,
    silver_run_id: str
) -> None:
    spark = get_spark_session("Silver States Transform")

    # 1) Meta
    ensure_manifest(spark)
    ensure_silver_metrics_table(spark)

    # 2) Descubrir qué procesar
    files = discover_new_files(spark, input_root=bronze_path, dataset="silver.states", pattern=pattern)
    if max_files:
        files = files[:max_files]
    if not files:
        print("[OK] No hay entradas nuevas para procesar en Silver States.")
        spark.stop()
        return
    print(f"[INFO] Ficheros/parquets a procesar ({len(files)}). Ejemplo: {files[:3]}")

    # 3) Leer Bronze (con basePath para exponer download_date)
    bronze = (
        spark.read.format("parquet")
             .option("basePath", bronze_path)
             .load(files)
    )

    # 4) Selección mínima necesaria para KPIs (crudos necesarios + meta)
    df = bronze.select(
        F.upper(F.trim(F.col("icao24"))).alias("icao24"),
        F.col("velocity").cast("double").alias("velocity_mps"),
        F.col("geo_altitude").cast("double").alias("geo_alt_m"),
        F.col("time_position").alias("time_pos_raw"),
        F.col("last_contact").alias("last_contact_raw"),
        "_ingest_id", "_ingest_ts", "_source_file", "download_ts", "download_date"
    )

    # 5) Tiempos y calendario
    def to_utc_ts(colname: str):
        c = F.col(colname)
        c_s = F.when(c > F.lit(10_000_000_000), (c / 1000).cast("long")).otherwise(c.cast("long"))
        return F.to_utc_timestamp(F.to_timestamp(c_s), "UTC")

    df = (df
        .withColumn("time_epoch", F.coalesce(F.col("time_pos_raw"), F.col("last_contact_raw")))
        .withColumn("time_ts", to_utc_ts("time_epoch"))
        .drop("time_pos_raw", "last_contact_raw", "time_epoch")
        .withColumn("event_date_utc", F.to_date(F.col("time_ts")))
        .withColumn("hour_of_day", F.hour(F.col("time_ts")).cast("tinyint"))
    )

    # 6) Validaciones / flags (solo lo necesario para KPIs de outliers)
    df = (df
        .withColumn("flag_velocity_outlier", (F.col("velocity_mps") < 0) | (F.col("velocity_mps") > 350))
        .withColumn("flag_alt_outlier", (F.col("geo_alt_m") < -500) | (F.col("geo_alt_m") > 20000))
    )

    # 7) Filtro de clave natural y dedupe por (icao24, time_ts)
    df = df.filter(F.col("icao24").isNotNull() & F.col("time_ts").isNotNull())
    before_dedupe = df.count()
    w = W.partitionBy("icao24", "time_ts").orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
    after_dedupe = df.count()

    # 8) Metadatos Silver y normalización de timestamp de silver
    df = add_silver_metadata(df, source_name="bronze.states", silver_version=silver_version)
    if "_silver_ts" not in df.columns and "_processing_date" in df.columns:
        df = df.withColumnRenamed("_processing_date", "_silver_ts")

    # 9) Esquema final (elimina lo innecesario, solo columnas para KPIs)
    KEEP = [
        "icao24", "time_ts", "event_date_utc", "hour_of_day",
        "velocity_mps", "geo_alt_m", "flag_velocity_outlier", "flag_alt_outlier",
        "_ingest_id", "_ingest_ts", "_source_file", "download_ts", "download_date",
        "_silver_version", "_silver_ts"
    ]
    df = df.select(*[c for c in KEEP if c in df.columns])

    # 10) Upsert a Delta (partición por fecha)
    merge_upsert(spark, df, silver_path, on_expr="t.icao24 = s.icao24 AND t.time_ts = s.time_ts", partition_cols=["event_date_utc"])

    # 11) Métricas Silver
    exclude_cols = ["_ingest_id","_ingest_ts","_source_file","download_ts","download_date","_silver_version","_silver_ts"]
    m = capture_metrics(
        df,
        dataset="silver.states",
        source_key=f"batch://silver.states/{silver_run_id}",
        ingest_id=silver_run_id,
        exclude_cols=exclude_cols
    )
    outlier_counts = {
        "velocity_outliers": df.filter(F.col("flag_velocity_outlier")).count(),
        "alt_outliers": df.filter(F.col("flag_alt_outlier")).count()
    }
    m = with_dup_ratio(m, duplicates_count=(before_dedupe - after_dedupe), rows_in=before_dedupe)
    m = with_freshness_lag_minutes(m, max_ingest_ts_col="_ingest_ts", df_out=df)
    m = with_extras(m, outlier_counts)
    write_metrics(spark, m, rows_out=after_dedupe, status="ok", notes="silver.states upsert ok")

    # 12) Manifest Silver -> done
    update_manifest(spark, files=files, dataset="silver.states", ingest_id=silver_run_id, status="done", silver_version=silver_version)

    # 13) Registrar tabla (opcional)
    if register_table:
        maybe_register_table(spark, silver_path, db="silver", table="states")

    print(f"[OK] silver.states: {after_dedupe} filas (dedupe de {before_dedupe}) → {silver_path} (Delta) + manifiesto actualizado. run_id={silver_run_id}")
    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Transformación SILVER de states (Bronze Delta -> Silver Delta)")
    parser.add_argument("--input", default=BRONZE_STATES_DEFAULT, help="Raíz BRONZE (p.ej. /opt/spark/lakehouse/bronze/states)")
    parser.add_argument("--output", default=SILVER_STATES_DEFAULT, help="Destino SILVER (p.ej. /opt/spark/lakehouse/silver/states)")
    parser.add_argument("--register-table", action="store_true", help="Registrar silver.states en el metastore si no existe")
    parser.add_argument("--max-files", type=int, default=None, help="(Opcional) límite de ficheros/parquets por ejecución")
    parser.add_argument("--pattern", default="download_date=*/*.parquet", help="Patrón glob en Bronze para descubrir candidatos")
    parser.add_argument("--silver-version", default=SILVER_VERSION_DEFAULT, help="Versión lógica del proceso Silver")
    parser.add_argument("--silver-run-id", default=SILVER_RUN_ID_DEFAULT, help="Identificador del run de Silver")
    args = parser.parse_args()

    main(
        bronze_path=args.input,
        silver_path=args.output,
        register_table=args.register_table,
        max_files=args.max_files,
        pattern=args.pattern,
        silver_version=args.silver_version,
        silver_run_id=args.silver_run_id
    )
