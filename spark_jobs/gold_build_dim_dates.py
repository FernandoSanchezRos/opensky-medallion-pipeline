import argparse
import uuid
import os
from pyspark.sql import functions as F, types as T

from spark_session import get_spark_session
from utils.init_gold_manifest import ensure_manifest, update_manifest
from utils.gold_metrics import ensure_gold_metrics_table, capture_metrics, write_metrics
from utils.gold_utils import add_gold_metadata

LAKE = os.getenv("SPARK_LAKEHOUSE", "/opt/spark/lakehouse")
GOLD_OUT = f"{LAKE}/gold/dim_dates"

GOLD_VERSION_DEFAULT = os.getenv("GOLD_VERSION", "v1.0.0")
GOLD_RUN_ID_DEFAULT  = os.getenv("GOLD_RUN_ID",  str(uuid.uuid4()))

def maybe_register_table(spark, output_path: str, db: str = "gold", table: str = "dim_dates") -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING delta LOCATION '{output_path}'")

def main(output_path: str, register_table: bool, gold_version: str, gold_run_id: str,
         years_back: int, years_fwd: int) -> None:

    spark = get_spark_session("Gold Dim Dates")
    ensure_manifest(spark)
    ensure_gold_metrics_table(spark)

    # Rango de fechas (relativo a hoy)
    today = F.current_date()
    start = F.date_sub(today, 365 * years_back)
    end   = F.date_add(today, 365 * years_fwd)

    # Genera fechas con sequence/explode (sin casts a int)
    df = (spark.createDataFrame([(1,)], "dummy int")
        .select(F.explode(F.sequence(start, end, F.expr("INTERVAL 1 DAY"))).alias("date"))
        .select(
            F.col("date"),
            ((F.dayofweek("date") + 5) % 7).cast("tinyint").alias("dow"),  # 0=Lunes
            F.weekofyear("date").alias("week"),
            F.date_format("date", "yyyy-MM").alias("month"),
            F.quarter("date").alias("quarter"),
            F.year("date").alias("year"),
            (F.dayofweek("date").isin([1, 7])).alias("is_weekend")
        )
    )

    rows_in = df.count()

    # Metadatos Gold
    df = add_gold_metadata(
        df,
        source_name="generated.dim_dates",
        gold_version=gold_version,
        run_id=gold_run_id,
        job_name="build_dim_dates"
    )

    # Escritura (overwrite completo)
    df.write.format("delta").mode("overwrite").save(output_path)

    # Métricas + manifest
    m = capture_metrics(
        df,
        dataset="gold.dim_dates",
        source_key=f"batch://gold.dim_dates/{gold_run_id}",
        ingest_id=gold_run_id,
        rows_in=rows_in
    )
    write_metrics(spark, m, rows_out=df.count(), status="ok")

    update_manifest(
        spark, files=[], dataset="gold.dim_dates",
        ingest_id=gold_run_id, status="done", gold_version=gold_version
    )

    if register_table:
        maybe_register_table(spark, output_path, db="gold", table="dim_dates")

    print(f"[OK] gold.dim_dates overwrite → {output_path}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Gold Dim Dates (tabla calendario)")
    p.add_argument("--output", default=GOLD_OUT)
    p.add_argument("--register-table", action="store_true")
    p.add_argument("--gold-version", default=GOLD_VERSION_DEFAULT)
    p.add_argument("--gold-run-id",  default=GOLD_RUN_ID_DEFAULT)
    p.add_argument("--years-back", type=int, default=3, help="Años hacia atrás")
    p.add_argument("--years-fwd",  type=int, default=1, help="Años hacia adelante")
    args = p.parse_args()
    main(args.output, args.register_table, args.gold_version, args.gold_run_id, args.years_back, args.years_fwd)
