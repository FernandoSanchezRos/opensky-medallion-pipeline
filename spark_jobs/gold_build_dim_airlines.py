import argparse
import uuid
import os
from pyspark.sql import functions as F

from spark_session import get_spark_session
from utils.init_gold_manifest import ensure_manifest, update_manifest
from utils.gold_metrics import ensure_gold_metrics_table, capture_metrics, write_metrics
from utils.gold_utils import add_gold_metadata

LAKE = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
FACT_FLIGHTS = f"{LAKE}/gold/fact_flights"
GOLD_OUT = f"{LAKE}/gold/dim_airlines"

GOLD_VERSION_DEFAULT = os.getenv("GOLD_VERSION", "v1.0.0")
GOLD_RUN_ID_DEFAULT  = os.getenv("GOLD_RUN_ID",  str(uuid.uuid4()))

def maybe_register_table(spark, output_path: str, db: str = "gold", table: str = "dim_airlines") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def main(output_path: str, register_table: bool, gold_version: str, gold_run_id: str) -> None:
    spark = get_spark_session("Gold Dim Airlines")
    ensure_manifest(spark)
    ensure_gold_metrics_table(spark)

    # Leer fact_flights
    df_flights = spark.read.format("delta").load(FACT_FLIGHTS)

    # Distinct aerolíneas
    df = (df_flights
        .select("airline_prefix", "airline_name")
        .where(F.col("airline_name").isNotNull())
        .dropDuplicates()
        .orderBy("airline_name")
    )

    rows_in = df.count()

    # Metadatos Gold
    df = add_gold_metadata(
        df,
        source_name="gold.fact_flights",
        gold_version=gold_version,
        run_id=gold_run_id,
        job_name="build_dim_airlines"
    )

    # Escritura (overwrite)
    df.write.format("delta").mode("overwrite").save(output_path)

    # Métricas + manifest
    m = capture_metrics(
        df,
        dataset="gold.dim_airlines",
        source_key=f"batch://gold.dim_airlines/{gold_run_id}",
        ingest_id=gold_run_id,
        rows_in=rows_in
    )
    write_metrics(spark, m, rows_out=df.count(), status="ok")

    update_manifest(
        spark, files=[], dataset="gold.dim_airlines",
        ingest_id=gold_run_id, status="done", gold_version=gold_version
    )

    if register_table:
        maybe_register_table(spark, output_path, db="gold", table="dim_airlines")

    print(f"[OK] gold.dim_airlines overwrite → {output_path}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Gold Dim Airlines")
    p.add_argument("--output", default=GOLD_OUT)
    p.add_argument("--register-table", action="store_true")
    p.add_argument("--gold-version", default=GOLD_VERSION_DEFAULT)
    p.add_argument("--gold-run-id",  default=GOLD_RUN_ID_DEFAULT)
    args = p.parse_args()
    main(args.output, args.register_table, args.gold_version, args.gold_run_id)
