import argparse
import uuid
import os

from pyspark.sql import functions as F
from pyspark.sql import Window as W
from delta.tables import DeltaTable

from spark_session import get_spark_session
from init_silver_manifest import ensure_manifest, update_manifest
from silver_metrics import (
    ensure_silver_metrics_table, capture_metrics, write_metrics,
    with_dup_ratio, with_unknown_pct, with_freshness_lag_minutes, with_extras
)
from silver_utils import add_silver_metadata

# --- Defaults ---
LAKE_DEFAULT = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
BRONZE_AIRLINES_DEFAULT = f"{LAKE_DEFAULT}/bronze/ref_airlines_prefixes"
BRONZE_AIRPORTS_DEFAULT = f"{LAKE_DEFAULT}/bronze/ref_airports"
SILVER_AIRLINES_DEFAULT = f"{LAKE_DEFAULT}/silver/ref_airlines_prefixes"
SILVER_AIRPORTS_DEFAULT = f"{LAKE_DEFAULT}/silver/ref_airports"

SILVER_VERSION_DEFAULT = os.getenv("SILVER_VERSION", "v1.0.0")
SILVER_RUN_ID_DEFAULT  = os.getenv("SILVER_RUN_ID", str(uuid.uuid4()))

def maybe_register_table(spark, output_path: str, db: str, table: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def overwrite_delta(df, dest: str):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(dest))

def compute_dataset_hash(df, ordered_cols):
    """
    Hash estable del contenido (tras limpieza/dedupe), independiente del orden físico.
    """
    if df.rdd.isEmpty():
        return "EMPTY"
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in ordered_cols]
    row_hash = F.sha2(F.concat_ws("␟", *cols), 256)
    agg = (df.select(row_hash.alias("h"))
             .agg(F.sha2(F.concat_ws("", F.sort_array(F.collect_list("h"))), 256).alias("hh"))
             .first())
    return agg["hh"]

# -------------------- Airlines Prefixes --------------------
def transform_airlines_prefixes(spark, bronze_path: str, silver_path: str, silver_version: str):
    bronze = spark.read.format("delta").load(bronze_path)

    df = (bronze
        .select(
            # limpieza
            F.upper(F.trim(F.col("prefix"))).alias("prefix"),
            F.when(F.length(F.trim(F.col("airline"))) > 0,
                   F.initcap(F.trim(F.col("airline")))).alias("airline_name"),
            # meta
            "_ingest_ts", "_source_file", "download_ts", "download_date"
        )
    )

    # Normaliza prefijo (solo letras, 2-4 chars). Vacío → NULL
    df = df.withColumn("prefix", F.regexp_extract(F.col("prefix"), r"^[A-Z]{2,4}", 0))
    df = df.withColumn("prefix", F.when(F.length("prefix") == 0, None).otherwise(F.col("prefix")))

    # Filtro clave y dedupe por prefix
    df = df.filter(F.col("prefix").isNotNull())
    before = df.count()
    w = W.partitionBy("prefix").orderBy(F.col("_ingest_ts").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
    after = df.count()

    # Metadatos Silver
    df = add_silver_metadata(df, source_name="bronze.ref_airlines_prefixes", silver_version=silver_version)
    if "_silver_ts" not in df.columns and "_processing_date" in df.columns:
        df = df.withColumnRenamed("_processing_date", "_silver_ts")

    # Esquema final (ligero)
    KEEP = [
        "prefix", "airline_name",
        "_ingest_ts", "_source_file", "download_ts", "download_date",
        "_silver_version", "_silver_ts"
    ]
    df = df.select(*[c for c in KEEP if c in df.columns])

    # Overwrite snapshot
    overwrite_delta(df, silver_path)

    # Métricas
    unknown = df.filter(F.col("airline_name").isNull()).count()
    m = capture_metrics(
        df,
        dataset="silver.ref_airlines_prefixes",
        source_key=f"snapshot://silver.ref_airlines_prefixes",
        ingest_id=SILVER_RUN_ID_DEFAULT,
        exclude_cols=["_ingest_ts","_source_file","download_ts","download_date","_silver_version","_silver_ts"]
    )
    m = with_dup_ratio(m, duplicates_count=(before - after), rows_in=before)
    m = with_unknown_pct(m, unknown_count=unknown, rows_out=after)
    m = with_freshness_lag_minutes(m, max_ingest_ts_col="_ingest_ts", df_out=df)
    m = with_extras(m, {"unknown_airline_name": unknown})
    write_metrics(spark, m, rows_out=after, status="ok", notes="silver.ref_airlines_prefixes snapshot ok")

    # Hash de contenido para manifest
    content_hash = compute_dataset_hash(df, ordered_cols=["prefix","airline_name"])
    return df, content_hash, after

# -------------------- Airports --------------------
def transform_airports(spark, bronze_path: str, silver_path: str, silver_version: str):
    bronze = spark.read.format("delta").load(bronze_path)

    df = (bronze
        .select(
            F.initcap(F.trim(F.col("city"))).alias("city"),
            F.upper(F.trim(F.col("region"))).alias("region"),
            F.upper(F.trim(F.col("icao"))).alias("icao"),
            F.upper(F.trim(F.col("iata"))).alias("iata"),
            F.initcap(F.trim(F.col("name"))).alias("name"),
            F.lower(F.trim(F.col("type"))).alias("type"),
            "_ingest_ts", "_source_file", "download_ts", "download_date"
        )
    )

    # Validaciones básicas
    df = df.withColumn("icao", F.when(F.col("icao").rlike(r"^[A-Z0-9]{4}$"), F.col("icao")))
    df = df.withColumn("iata", F.when(F.col("iata").rlike(r"^[A-Z0-9]{3}$"), F.col("iata")))
    df = df.filter(F.col("icao").isNotNull())  # clave obligatoria

    # Dedupe por ICAO (el más reciente por _ingest_ts)
    before = df.count()
    w = W.partitionBy("icao").orderBy(F.col("_ingest_ts").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
    after = df.count()

    # Metadatos Silver
    df = add_silver_metadata(df, source_name="bronze.ref_airports", silver_version=silver_version)
    if "_silver_ts" not in df.columns and "_processing_date" in df.columns:
        df = df.withColumnRenamed("_processing_date", "_silver_ts")

    # Esquema final
    KEEP = [
        "city","region","icao","iata","name","type",
        "_ingest_ts","_source_file","download_ts","download_date",
        "_silver_version","_silver_ts"
    ]
    df = df.select(*[c for c in KEEP if c in df.columns])

    # Overwrite snapshot
    overwrite_delta(df, silver_path)

    # Métricas
    unknown_iata = df.filter(F.col("iata").isNull()).count()
    m = capture_metrics(
        df,
        dataset="silver.ref_airports",
        source_key=f"snapshot://silver.ref_airports",
        ingest_id=SILVER_RUN_ID_DEFAULT,
        exclude_cols=["_ingest_ts","_source_file","download_ts","download_date","_silver_version","_silver_ts"]
    )
    m = with_dup_ratio(m, duplicates_count=(before - after), rows_in=before)
    m = with_unknown_pct(m, unknown_count=unknown_iata, rows_out=after)
    m = with_freshness_lag_minutes(m, max_ingest_ts_col="_ingest_ts", df_out=df)
    m = with_extras(m, {"unknown_iata": unknown_iata})
    write_metrics(spark, m, rows_out=after, status="ok", notes="silver.ref_airports snapshot ok")

    # Hash de contenido para manifest
    content_hash = compute_dataset_hash(df, ordered_cols=["icao","iata","name","city","region","type"])
    return df, content_hash, after

# -------------------- Main --------------------
def main(dataset: str,
         bronze_input: str,
         silver_output: str,
         register_table: bool,
         silver_version: str,
         silver_run_id: str):
    spark = get_spark_session(f"Silver Static: {dataset}")
    ensure_manifest(spark)
    ensure_silver_metrics_table(spark)

    if dataset == "airlines_prefixes":
        bronze_path = bronze_input or BRONZE_AIRLINES_DEFAULT
        silver_path = silver_output or SILVER_AIRLINES_DEFAULT
        df, content_hash, rows = transform_airlines_prefixes(spark, bronze_path, silver_path, silver_version)
        manifest_dataset = "silver.ref_airlines_prefixes"
        table_name = "ref_airlines_prefixes"
    elif dataset == "airports":
        bronze_path = bronze_input or BRONZE_AIRPORTS_DEFAULT
        silver_path = silver_output or SILVER_AIRPORTS_DEFAULT
        df, content_hash, rows = transform_airports(spark, bronze_path, silver_path, silver_version)
        manifest_dataset = "silver.ref_airports"
        table_name = "ref_airports"
    else:
        raise ValueError("dataset debe ser 'airlines_prefixes' o 'airports'.")

    # Manifest (snapshot): usa el hash de contenido para idempotencia + versionado
    update_manifest(
        spark,
        files=[bronze_path],                # origen lógico del snapshot
        dataset=manifest_dataset,
        ingest_id=silver_run_id,
        status="done",
        file_hash=content_hash,            # contenido canonizado
        silver_version=silver_version
    )

    if register_table:
        maybe_register_table(spark, silver_path, db="silver", table=table_name)

    print(f"[OK] {manifest_dataset}: {rows} filas → {silver_path} (Delta, overwrite). run_id={silver_run_id}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformación SILVER de estáticos (snapshot limpio desde Bronze)")
    parser.add_argument("--dataset", required=True, choices=["airlines_prefixes","airports"])
    parser.add_argument("--bronze-input", default=None, help="Ruta Delta Bronze (si no, default)")
    parser.add_argument("--silver-output", default=None, help="Ruta Delta Silver (si no, default)")
    parser.add_argument("--register-table", action="store_true", help="Registrar la tabla en el metastore")
    parser.add_argument("--silver-version", default=SILVER_VERSION_DEFAULT)
    parser.add_argument("--silver-run-id", default=SILVER_RUN_ID_DEFAULT)
    args = parser.parse_args()

    main(
        dataset=args.dataset,
        bronze_input=args.bronze_input,
        silver_output=args.silver_output,
        register_table=args.register_table,
        silver_version=args.silver_version,
        silver_run_id=args.silver_run_id
    )
