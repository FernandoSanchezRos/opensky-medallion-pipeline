import argparse
import uuid
import os
from pyspark.sql import functions as F

from spark_session import get_spark_session
from utils.init_gold_manifest import ensure_manifest, update_manifest
from utils.gold_metrics import (
    ensure_gold_metrics_table, capture_metrics, write_metrics, with_extras
)
from utils.gold_utils import add_gold_metadata, merge_upsert

# --- Defaults (env) ---
LAKE = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
SILVER_STATES = f"{LAKE}/silver/states"
GOLD_OUT      = f"{LAKE}/gold/fact_states"

GOLD_VERSION_DEFAULT = os.getenv("GOLD_VERSION", "v1.0.0")
GOLD_RUN_ID_DEFAULT  = os.getenv("GOLD_RUN_ID",  str(uuid.uuid4()))
EXPECTED_PINGS_PER_DAY = int(os.getenv("EXPECTED_PINGS_PER_DAY","960"))  # 24h*3600/90s = 960 si muestreo ~90s

def maybe_register_table(spark, output_path: str, db: str = "gold", table: str = "fact_states") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def main(silver_states: str, output_path: str, register_table: bool, gold_version: str, gold_run_id: str) -> None:
    spark = get_spark_session("Gold Fact States")

    # 1) Infra
    ensure_manifest(spark)
    ensure_gold_metrics_table(spark)

    # 2) Leer Silver (según tu esquema real)
    # schema: icao24, time_ts, hour_of_day, velocity_mps, geo_alt_m,
    #         flag_velocity_outlier, flag_alt_outlier, _ingest_id, _ingest_ts,
    #         _source_file, download_ts, download_date, _silver_version, _silver_ts, event_date_utc
    src = (spark.read.format("delta").load(silver_states)
           .select(
                "icao24","time_ts","event_date_utc","hour_of_day",
                "velocity_mps","geo_alt_m",
                "flag_velocity_outlier","flag_alt_outlier",
                "_ingest_id","_ingest_ts","_source_file","download_ts","download_date",
                "_silver_version","_silver_ts"
           )
           .where(F.col("icao24").isNotNull() & F.col("event_date_utc").isNotNull())
    )

    rows_in = src.count()

    # 3) Agregados por día y aeronave
    # - pings_count
    # - cobertura estimada (pings / EXPECTED_PINGS_PER_DAY * 100)
    # - percentiles 95 de velocidad y altitud
    # - % de outliers (flags) sobre el total del día
    # - lineage (máximos por grupo para _ingest_ts/_silver_ts y última download_date)
    agg = (src.groupBy("icao24","event_date_utc")
        .agg(
            F.count(F.lit(1)).alias("pings_count"),
            F.expr("percentile_approx(velocity_mps, 0.95)").alias("p95_velocity"),
            F.expr("percentile_approx(geo_alt_m, 0.95)").alias("p95_altitude"),
            F.avg(F.col("flag_velocity_outlier").cast("double")).alias("pct_velocity_outlier"),
            F.avg(F.col("flag_alt_outlier").cast("double")).alias("pct_alt_outlier"),
            F.max("_ingest_ts").alias("_ingest_ts"),
            F.max("_silver_ts").alias("_silver_ts"),
            F.max("download_date").alias("download_date")
        )
        .withColumn("coverage_pct", F.col("pings_count")/F.lit(EXPECTED_PINGS_PER_DAY)*100.0)
    )

    # 4) Metadatos Gold
    df = add_gold_metadata(
        agg,
        source_name="silver.states",
        gold_version=gold_version,
        run_id=gold_run_id,
        job_name="build_fact_states"
    )

    # 5) Upsert (Delta) por clave natural (icao24, event_date_utc) y partición por event_date_utc
    merge_upsert(
        spark, df, output_path,
        keys_expr="t.icao24 = s.icao24 AND t.event_date_utc = s.event_date_utc",
        partition_cols=["event_date_utc"]
    )

    # 6) Métricas + manifest
    m = capture_metrics(
        df,
        dataset="gold.fact_states",
        source_key=f"batch://gold.fact_states/{gold_run_id}",
        ingest_id=gold_run_id,
        rows_in=rows_in
    )
    m = with_extras(m, {"expected_pings_per_day": EXPECTED_PINGS_PER_DAY})
    write_metrics(spark, m, rows_out=df.count(), status="ok")

    update_manifest(
        spark, files=[], dataset="gold.fact_states",
        ingest_id=gold_run_id, status="done", gold_version=gold_version
    )

    if register_table:
        maybe_register_table(spark, output_path, db="gold", table="fact_states")

    print(f"[OK] gold.fact_states upsert → {output_path}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Gold Fact States (desde Silver)")
    p.add_argument("--silver-states", default=SILVER_STATES)
    p.add_argument("--output", default=GOLD_OUT)
    p.add_argument("--register-table", action="store_true")
    p.add_argument("--gold-version", default=GOLD_VERSION_DEFAULT)
    p.add_argument("--gold-run-id",  default=GOLD_RUN_ID_DEFAULT)
    args = p.parse_args()
    main(args.silver_states, args.output, args.register_table, args.gold_version, args.gold_run_id)
