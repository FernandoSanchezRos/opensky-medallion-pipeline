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
    with_dup_ratio, with_freshness_lag_minutes, with_unknown_pct, with_extras
)
from silver_utils import add_silver_metadata

# --- Config por defecto via env ---
LAKE_DEFAULT = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
BRONZE_ARR_DEFAULT   = f"{LAKE_DEFAULT}/bronze/flights_arrival"
SILVER_ARR_DEFAULT   = f"{LAKE_DEFAULT}/silver/flights_arrival"
REF_AIRLINES_DEFAULT = f"{LAKE_DEFAULT}/bronze/ref_airlines_prefixes"
REF_AIRPORTS_DEFAULT = f"{LAKE_DEFAULT}/bronze/ref_airports"

SILVER_VERSION_DEFAULT = os.getenv("SILVER_VERSION", "v1.0.0")
SILVER_RUN_ID_DEFAULT  = os.getenv("SILVER_RUN_ID", str(uuid.uuid4()))

def maybe_register_table(spark, output_path: str, db: str = "silver", table: str = "flights_arrival") -> None:
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

def to_utc_ts(colname: str):
    c = F.col(colname)
    c_s = F.when(c > F.lit(10_000_000_000), (c/1000).cast("long")).otherwise(c.cast("long"))
    return F.to_utc_timestamp(F.to_timestamp(c_s), "UTC")

def main(
    bronze_path: str,
    silver_path: str,
    ref_airlines_path: str,
    ref_airports_path: str,
    register_table: bool,
    max_files: int | None,
    pattern: str,
    silver_version: str,
    silver_run_id: str
) -> None:
    spark = get_spark_session("Silver Flights Arrival Transform")

    # 1) Meta
    ensure_manifest(spark)
    ensure_silver_metrics_table(spark)

    # 2) Descubrir qué procesar (parquet físicos de Bronze)
    files = discover_new_files(spark, input_root=bronze_path, dataset="silver.flights_arrival", pattern=pattern)
    if max_files:
        files = files[:max_files]
    if not files:
        print("[OK] No hay entradas nuevas para procesar en silver.flights_arrival.")
        spark.stop()
        return
    print(f"[INFO] Parquets a procesar ({len(files)}). Ejemplo: {files[:3]}")

    # 3) Leer Bronze (con basePath para exponer download_date)
    bronze = (
        spark.read.format("parquet")
             .option("basePath", bronze_path)
             .load(files)
    )

    # 4) Normalización básica y renombres
    df = bronze.select(
        F.upper(F.trim(F.col("icao24"))).alias("icao24"),
        F.when(F.length(F.trim(F.col("callsign"))) > 0, F.upper(F.trim(F.col("callsign")))).alias("callsign_norm"),
        F.upper(F.trim(F.col("estDepartureAirport"))).alias("dep_icao"),
        F.upper(F.trim(F.col("estArrivalAirport"))).alias("arr_icao"),
        F.col("firstSeen").alias("first_seen_raw"),
        F.col("lastSeen").alias("last_seen_raw"),
        # metas de bronze
        "_ingest_id","_ingest_ts","_source_file","download_ts","download_date"
    )

    # 5) Tiempos y calendario + block time
    df = (df
        .withColumn("first_seen_ts", to_utc_ts("first_seen_raw"))
        .withColumn("last_seen_ts",  to_utc_ts("last_seen_raw"))
        .drop("first_seen_raw","last_seen_raw")
        .withColumn("block_time_s", (F.unix_timestamp("last_seen_ts") - F.unix_timestamp("first_seen_ts")).cast("int"))
        # Calendarios (dep=first_seen_ts, arr=last_seen_ts)
        .withColumn("dep_date_utc", F.to_date("first_seen_ts"))
        .withColumn("arr_date_utc", F.to_date("last_seen_ts"))
        .withColumn("dep_hour", F.hour("first_seen_ts").cast("tinyint"))
        .withColumn("arr_hour", F.hour("last_seen_ts").cast("tinyint"))
        .withColumn("dep_dow", F.dayofweek("first_seen_ts").cast("tinyint"))
        .withColumn("arr_dow", F.dayofweek("last_seen_ts").cast("tinyint"))
        .withColumn("dep_week", F.weekofyear("first_seen_ts"))
        .withColumn("arr_week", F.weekofyear("last_seen_ts"))
        .withColumn("dep_month", F.date_format("first_seen_ts","yyyy-MM"))
        .withColumn("arr_month", F.date_format("last_seen_ts","yyyy-MM"))
    )

    # 6) Route + flight_id
    df = df.withColumn("route_key", F.concat_ws("→", F.col("dep_icao"), F.col("arr_icao")))
    df = df.withColumn(
        "flight_id",
        F.sha2(F.concat_ws("|", "icao24", "dep_icao", "arr_icao",
                           F.col("first_seen_ts").cast("string"), F.col("last_seen_ts").cast("string")), 256)
    )

    # 7) Enriquecimiento: aerolíneas (por prefijo de callsign) y aeropuertos (IATA)
    ref_airlines = (spark.read.format("delta").load(ref_airlines_path)
                    .select(F.upper(F.trim(F.col("prefix"))).alias("prefix"), F.col("airline").alias("airline_name")))
    ref_airports = (spark.read.format("delta").load(ref_airports_path)
                    .select(F.upper(F.trim(F.col("icao"))).alias("icao"), F.col("iata")))

    # prefijo = letras iniciales del callsign (ej. "IBE1234" -> "IBE")
    df = df.withColumn("airline_prefix", F.regexp_extract(F.col("callsign_norm"), r"^[A-Z]+", 0))
    df = (df
          .join(F.broadcast(ref_airlines), df.airline_prefix == ref_airlines.prefix, "left")
          .drop("prefix"))

    df = (df
          .join(F.broadcast(ref_airports).withColumnRenamed("icao","dep_icao_ref"),
                df.dep_icao == F.col("dep_icao_ref"), "left")
          .withColumnRenamed("iata","dep_iata")
          .drop("dep_icao_ref"))

    df = (df
          .join(F.broadcast(ref_airports).withColumnRenamed("icao","arr_icao_ref"),
                df.arr_icao == F.col("arr_icao_ref"), "left")
          .withColumnRenamed("iata","arr_iata")
          .drop("arr_icao_ref"))

    # Unknown flags
    df = (df
        .withColumn("unknown_dep", F.col("dep_icao").isNull() | F.col("dep_iata").isNull())
        .withColumn("unknown_arr", F.col("arr_icao").isNull() | F.col("arr_iata").isNull())
    )

    # 8) Filtro de clave y dedupe por flight_id
    df = df.filter(F.col("flight_id").isNotNull() & F.col("first_seen_ts").isNotNull() & F.col("last_seen_ts").isNotNull())
    before_dedupe = df.count()
    w = W.partitionBy("flight_id").orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn")==1).drop("_rn")
    after_dedupe = df.count()

    # 9) Metadatos Silver
    df = add_silver_metadata(df, source_name="bronze.flights_arrival", silver_version=silver_version)
    if "_silver_ts" not in df.columns and "_processing_date" in df.columns:
        df = df.withColumnRenamed("_processing_date", "_silver_ts")

    # 10) Esquema final (solo lo necesario para KPIs)
    KEEP = [
        "flight_id","icao24","callsign_norm","airline_prefix","airline_name",
        "dep_icao","dep_iata","arr_icao","arr_iata",
        "unknown_dep","unknown_arr",
        "first_seen_ts","last_seen_ts","block_time_s",
        "dep_date_utc","arr_date_utc","dep_hour","arr_hour","dep_dow","arr_dow","dep_week","arr_week","dep_month","arr_month",
        "route_key",
        "_ingest_ts","_source_file","download_ts","download_date","_silver_version","_silver_ts"
    ]
    df = df.select(*[c for c in KEEP if c in df.columns])

    # 11) MERGE → partición por arr_date_utc
    merge_upsert(spark, df, silver_path, on_expr="t.flight_id = s.flight_id", partition_cols=["arr_date_utc"])

    # 12) Métricas Silver
    unknown_count = df.filter(F.col("unknown_dep") | F.col("unknown_arr")).count()
    m = capture_metrics(
        df, dataset="silver.flights_arrival",
        source_key=f"batch://silver.flights_arrival/{silver_run_id}",
        ingest_id=silver_run_id,
        exclude_cols=["_ingest_ts","_source_file","download_ts","download_date","_silver_version","_silver_ts"]
    )
    m = with_dup_ratio(m, duplicates_count=(before_dedupe - after_dedupe), rows_in=before_dedupe)
    m = with_unknown_pct(m, unknown_count=unknown_count, rows_out=after_dedupe)
    m = with_freshness_lag_minutes(m, max_ingest_ts_col="_ingest_ts", df_out=df)
    m = with_extras(m, {"arrivals_rows": after_dedupe})
    write_metrics(spark, m, rows_out=after_dedupe, status="ok", notes="silver.flights_arrival upsert ok")

    # 13) Manifest
    update_manifest(spark, files=files, dataset="silver.flights_arrival",
                    ingest_id=silver_run_id, status="done", silver_version=silver_version)

    # 14) Registrar tabla (opcional)
    if register_table:
        maybe_register_table(spark, silver_path, db="silver", table="flights_arrival")

    print(f"[OK] silver.flights_arrival: {after_dedupe} filas (dedupe de {before_dedupe}) → {silver_path}. run_id={silver_run_id}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transformación SILVER de flights_arrival (Bronze -> Silver)")
    parser.add_argument("--input", default=BRONZE_ARR_DEFAULT, help="Raíz BRONZE (p.ej. /opt/spark/lakehouse/bronze/flights_arrival)")
    parser.add_argument("--output", default=SILVER_ARR_DEFAULT, help="Destino SILVER (p.ej. /opt/spark/lakehouse/silver/flights_arrival)")
    parser.add_argument("--ref-airlines", default=REF_AIRLINES_DEFAULT, help="Ruta Delta ref_airlines_prefixes")
    parser.add_argument("--ref-airports", default=REF_AIRPORTS_DEFAULT, help="Ruta Delta ref_airports")
    parser.add_argument("--register-table", action="store_true", help="Registrar silver.flights_arrival en el metastore si no existe")
    parser.add_argument("--max-files", type=int, default=None, help="(Opcional) límite de ficheros/parquets por ejecución")
    parser.add_argument("--pattern", default="download_date=*/*.parquet", help="Patrón glob en Bronze para descubrir candidatos")
    parser.add_argument("--silver-version", default=SILVER_VERSION_DEFAULT, help="Versión lógica del proceso Silver")
    parser.add_argument("--silver-run-id", default=SILVER_RUN_ID_DEFAULT, help="Identificador del run de Silver")
    args = parser.parse_args()

    main(
        bronze_path=args.input,
        silver_path=args.output,
        ref_airlines_path=args.ref_airlines,
        ref_airports_path=args.ref_airports,
        register_table=args.register_table,
        max_files=args.max_files,
        pattern=args.pattern,
        silver_version=args.silver_version,
        silver_run_id=args.silver_run_id
    )
