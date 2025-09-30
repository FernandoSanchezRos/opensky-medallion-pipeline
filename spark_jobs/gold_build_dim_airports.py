import argparse
import uuid
import os
from pyspark.sql import functions as F

from spark_session import get_spark_session
from utils.init_gold_manifest import ensure_manifest, update_manifest
from utils.gold_metrics import ensure_gold_metrics_table, capture_metrics, write_metrics
from utils.gold_utils import add_gold_metadata, merge_upsert

# Paths por defecto (env)
LAKE = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
SILVER_SRC = f"{LAKE}/silver/ref_airports"
GOLD_OUT   = f"{LAKE}/gold/dim_airports"

GOLD_VERSION_DEFAULT = os.getenv("GOLD_VERSION", "v1.0.0")
GOLD_RUN_ID_DEFAULT  = os.getenv("GOLD_RUN_ID",  str(uuid.uuid4()))

def maybe_register_table(spark, output_path: str, db: str = "gold", table: str = "dim_airports") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def main(silver_airports: str, output_path: str, register_table: bool, gold_version: str, gold_run_id: str) -> None:
    spark = get_spark_session("Gold Dim Airports")
    ensure_manifest(spark)
    ensure_gold_metrics_table(spark)

    # === Silver ref_airports schema (tuyo):
    # city, region, icao, iata, name, type, _ingest_ts, _source_file, download_ts, download_date, _silver_version, _silver_ts
    src = (spark.read.format("delta").load(silver_airports)
           .select(
               "icao","iata","name","city","region","type",
               "_ingest_ts","download_date","_silver_version","_silver_ts"
           )
           .where(F.col("icao").isNotNull())
          )

    rows_in = src.count()

    # Normalización ligera (trim/upper en códigos)
    df = (src
          .withColumn("icao", F.upper(F.trim("icao")))
          .withColumn("iata", F.upper(F.trim("iata")))
          .dropDuplicates(["icao"])
    )

    # Metadatos Gold
    df = add_gold_metadata(
        df,
        source_name="silver.ref_airports",
        gold_version=gold_version,
        run_id=gold_run_id,
        job_name="build_dim_airports"
    )

    # Upsert por clave natural ICAO (sin partición)
    merge_upsert(
        spark, df, output_path,
        keys_expr="t.icao = s.icao",
        partition_cols=None
    )

    # Métricas + manifest
    m = capture_metrics(
        df,
        dataset="gold.dim_airports",
        source_key=f"batch://gold.dim_airports/{gold_run_id}",
        ingest_id=gold_run_id,
        rows_in=rows_in
    )
    write_metrics(spark, m, rows_out=df.count(), status="ok")

    update_manifest(
        spark, files=[], dataset="gold.dim_airports",
        ingest_id=gold_run_id, status="done", gold_version=gold_version
    )

    if register_table:
        maybe_register_table(spark, output_path, db="gold", table="dim_airports")

    print(f"[OK] gold.dim_airports upsert → {output_path}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Gold Dim Airports (desde Silver ref_airports)")
    p.add_argument("--silver-airports", default=SILVER_SRC)
    p.add_argument("--output", default=GOLD_OUT)
    p.add_argument("--register-table", action="store_true")
    p.add_argument("--gold-version", default=GOLD_VERSION_DEFAULT)
    p.add_argument("--gold-run-id",  default=GOLD_RUN_ID_DEFAULT)
    args = p.parse_args()
    main(args.silver_airports, args.output, args.register_table, args.gold_version, args.gold_run_id)
