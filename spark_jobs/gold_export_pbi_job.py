# spark_jobs/gold_export_pbi_job.py
from __future__ import annotations
import argparse
from utils.spark_session import get_spark_session


def export_table(spark, delta_path: str, out_path: str, single_file: bool = True):
    """
    Lee una tabla Delta y la exporta a Parquet (snapshot para Power BI).
    Si single_file=True, fuerza coalesce(1) para dejar un único fichero .parquet.
    """
    df = spark.read.format("delta").load(delta_path)
    if single_file:
        df = df.coalesce(1)
    (
        df.write
          .mode("overwrite")
          .parquet(out_path)
    )


def main():
    parser = argparse.ArgumentParser(description="Exporta tablas Gold a Parquet para Power BI")
    parser.add_argument("--gold-root", default="/opt/spark/lakehouse/gold",
                        help="Ruta base de las tablas Gold en Delta")
    parser.add_argument("--out-root", default="/opt/spark/lakehouse/pbi_exports",
                        help="Ruta base de salida para los Parquet de BI")
    parser.add_argument("--tables", nargs="+", default=[
        "fact_flights",
        "fact_states",
        "dim_airports",
        "dim_airlines_prefixes",
        "dim_airlines",
        "dim_dates"
    ], help="Lista de tablas a exportar")
    parser.add_argument("--single-file", action="store_true",
                        help="Si se indica, fuerza coalesce(1) para un único fichero por tabla")

    args = parser.parse_args()

    spark = get_spark_session("gold_export_pbi")

    for t in args.tables:
        delta_path = f"{args.gold_root}/{t}"
        out_path   = f"{args.out_root}/{t}"
        print(f"[EXPORT] {delta_path} -> {out_path} (single_file={args.single_file})")
        export_table(spark, delta_path, out_path, single_file=args.single_file)

    spark.stop()


if __name__ == "__main__":
    main()
