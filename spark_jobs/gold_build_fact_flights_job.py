import argparse
import uuid
import os
from pyspark.sql import functions as F, Window as W

from spark_session import get_spark_session
from utils.init_gold_manifest import ensure_manifest, update_manifest
from utils.gold_metrics import (
    ensure_gold_metrics_table, capture_metrics, write_metrics
)
from utils.gold_utils import add_gold_metadata, merge_upsert

# --- Defaults (env) ---
LAKE = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
SILVER_DEP = f"{LAKE}/silver/flights_departure"
SILVER_ARR = f"{LAKE}/silver/flights_arrival"
GOLD_OUT   = f"{LAKE}/gold/fact_flights"

GOLD_VERSION_DEFAULT = os.getenv("GOLD_VERSION", "v1.0.0")
GOLD_RUN_ID_DEFAULT  = os.getenv("GOLD_RUN_ID",  str(uuid.uuid4()))

def maybe_register_table(spark, output_path: str, db: str = "gold", table: str = "fact_flights") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def main(
    silver_dep: str,
    silver_arr: str,
    output_path: str,
    register_table: bool,
    gold_version: str,
    gold_run_id: str,
) -> None:
    spark = get_spark_session("Gold Fact Flights")

    # 1) Infra: manifest + metrics
    ensure_manifest(spark)
    ensure_gold_metrics_table(spark)

    # 2) Leer Silver (esquema ya consolidado en Silver)
    base_cols = [
        "flight_id","icao24","callsign_norm","airline_prefix","airline_name",
        "dep_icao","dep_iata","arr_icao","arr_iata",
        "unknown_dep","unknown_arr",
        "first_seen_ts","last_seen_ts","block_time_s",
        "dep_date_utc","arr_date_utc","dep_hour","arr_hour","dep_dow","arr_dow",
        "dep_week","arr_week","dep_month","arr_month","route_key",
        "_ingest_ts","_source_file","download_ts","download_date",
        "_silver_version","_silver_ts"
    ]

    dep = spark.read.format("delta").load(silver_dep).select(*[c for c in base_cols if c in spark.read.format("delta").load(silver_dep).columns])
    arr = spark.read.format("delta").load(silver_arr).select(*[c for c in base_cols if c in spark.read.format("delta").load(silver_arr).columns])

    rows_in = dep.count() + arr.count()

    # 3) Unificar y deduplicar por flight_id -> mantener fila más fresca por _silver_ts
    df = dep.unionByName(arr, allowMissingColumns=True)

    w = W.partitionBy("flight_id").orderBy(F.col("_silver_ts").desc_nulls_last(), F.col("_ingest_ts").desc_nulls_last())
    df = (df
          .withColumn("_rn", F.row_number().over(w))
          .where(F.col("_rn") == 1)
          .drop("_rn"))

    # 4) Metadatos Gold
    df = add_gold_metadata(
        df,
        source_name="silver.flights_departure+arrival",
        gold_version=gold_version,
        run_id=gold_run_id,
        job_name="build_fact_flights"
    )

    # 5) Upsert (Delta) por PK flight_id, partición por dep_date_utc
    merge_upsert(
        spark, df, output_path,
        keys_expr="t.flight_id = s.flight_id",
        partition_cols=["dep_date_utc"]
    )

    # 6) Métricas + manifest
    m = capture_metrics(
        df,
        dataset="gold.fact_flights",
        source_key=f"batch://gold.fact_flights/{gold_run_id}",
        ingest_id=gold_run_id,
        rows_in=rows_in
    )
    write_metrics(spark, m, rows_out=df.count(), status="ok")

    update_manifest(
        spark, files=[], dataset="gold.fact_flights",
        ingest_id=gold_run_id, status="done", gold_version=gold_version
    )

    # 7) Registrar tabla (opcional)
    if register_table:
        maybe_register_table(spark, output_path, db="gold", table="fact_flights")

    print(f"[OK] gold.fact_flights upsert → {output_path}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Gold Fact Flights (consolidación desde Silver)")
    p.add_argument("--silver-dep", default=SILVER_DEP)
    p.add_argument("--silver-arr", default=SILVER_ARR)
    p.add_argument("--output", default=GOLD_OUT)
    p.add_argument("--register-table", action="store_true")
    p.add_argument("--gold-version", default=GOLD_VERSION_DEFAULT)
    p.add_argument("--gold-run-id",  default=GOLD_RUN_ID_DEFAULT)
    args = p.parse_args()
    main(args.silver_dep, args.silver_arr, args.output, args.register_table, args.gold_version, args.gold_run_id)
