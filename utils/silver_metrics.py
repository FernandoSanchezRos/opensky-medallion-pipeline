from pyspark.sql import DataFrame, functions as F, types as T
from typing import Iterable, Optional, Dict, Any
import os, json
from datetime import datetime

SILVER_METRICS_PATH = os.getenv("SILVER_METRICS_PATH", "/opt/spark/lakehouse/meta/silver_metrics")

# 1) Tabla de métricas (gemela a Bronze) + compatible con extras de Silver
def ensure_silver_metrics_table(spark) -> None:
    schema = T.StructType([
        T.StructField("dataset",      T.StringType(),   False),
        T.StructField("source_key",   T.StringType(),   False),  # en Silver puede ser partition/date o _ingest_id (más genérico que 'source_file')
        T.StructField("ingest_id",    T.StringType(),   False),  # run_id de Silver
        T.StructField("processed_ts", T.TimestampType(),False),

        T.StructField("rows_in",      T.LongType(),     True),
        T.StructField("rows_out",     T.LongType(),     True),
        T.StructField("corrupt_rows", T.LongType(),     True),

        T.StructField("nulls_json",   T.StringType(),   True),  # {"col": count, ...}
        T.StructField("columns_json", T.StringType(),   True),  # [{"name":..., "type":...}, ...]

        # --- Extras típicos Silver (opcionales) ---
        T.StructField("dup_ratio",            T.DoubleType(),   True),  # duplicados detectados/rows_in
        T.StructField("unknown_pct",          T.DoubleType(),   True),  # ej. %unknown_dep/arr
        T.StructField("freshness_lag_minutes",T.DoubleType(),   True),  # now - max(_ingest_ts) o similar
        T.StructField("extras_json",          T.StringType(),   True),  # libre para métricas ad-hoc

        T.StructField("status",       T.StringType(),   True),  # ok|warn|fail
        T.StructField("notes",        T.StringType(),   True),
    ])
    try:
        spark.read.format("delta").load(SILVER_METRICS_PATH).limit(1).collect()
        print(f"[INFO] silver_metrics ya existe en {SILVER_METRICS_PATH}")
    except Exception:
        print(f"[INFO] Creando silver_metrics en {SILVER_METRICS_PATH}")
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(SILVER_METRICS_PATH)

# 2) Captura métrica simple (idéntica a Bronze, con nombre de clave flexible)
def capture_metrics(
    df: DataFrame,
    dataset: str,
    source_key: str,                  # en Silver puede ser la partición (p.ej. 'arr_date_utc=2025-09-27') o un _ingest_id
    ingest_id: str,
    exclude_cols: Optional[Iterable[str]] = None,
    rows_in: Optional[int] = None,
) -> Dict[str, Any]:
    d = df.cache()
    rows_in = rows_in if rows_in is not None else d.count()

    corrupt = d.filter(F.col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in d.columns else 0

    excl = set(exclude_cols or [])
    cols_for_nulls = [c for c in d.columns if c not in excl]
    if cols_for_nulls:
        agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).cast("long").alias(c) for c in cols_for_nulls]
        row = d.agg(*agg_exprs).first()
        nulls_map = {c: int(row[c] or 0) for c in cols_for_nulls}
    else:
        nulls_map = {}

    cols_json = json.dumps(
        [{"name": f.name, "type": f.dataType.simpleString()} for f in d.schema.fields],
        ensure_ascii=False
    )

    d.unpersist()

    return {
        "dataset": dataset,
        "source_key": source_key,
        "ingest_id": ingest_id,
        "processed_ts": None,

        "rows_in": int(rows_in),
        "rows_out": None,
        "corrupt_rows": int(corrupt),

        "nulls_json": json.dumps(nulls_map, ensure_ascii=False),
        "columns_json": cols_json,

        # extras opcionales: rellénalos con helpers si te interesan
        "dup_ratio": None,
        "unknown_pct": None,
        "freshness_lag_minutes": None,
        "extras_json": None,

        "status": None,
        "notes": None,
    }

# 3) Helpers opcionales para completar extras (úsalos solo si aplican a tu dataset)
def with_dup_ratio(metrics: Dict[str, Any], duplicates_count: Optional[int], rows_in: Optional[int]) -> Dict[str, Any]:
    if duplicates_count is not None and rows_in and rows_in > 0:
        metrics = dict(metrics)
        metrics["dup_ratio"] = float(duplicates_count) / float(rows_in)
    return metrics

def with_unknown_pct(metrics: Dict[str, Any], unknown_count: Optional[int], rows_out: Optional[int]) -> Dict[str, Any]:
    if unknown_count is not None and rows_out and rows_out > 0:
        metrics = dict(metrics)
        metrics["unknown_pct"] = float(unknown_count) / float(rows_out)
    return metrics

def with_freshness_lag_minutes(metrics: Dict[str, Any], max_ingest_ts_col: Optional[str], df_out: Optional[DataFrame]) -> Dict[str, Any]:
    """
    Calcula lag aproximado en minutos como now() - max(_ingest_ts) del df_out si pasas el nombre de la columna.
    """
    if df_out is not None and max_ingest_ts_col and max_ingest_ts_col in df_out.columns:
        row = df_out.agg(F.max(F.col(max_ingest_ts_col)).alias("mx")).first()
        if row and row["mx"] is not None:
            # en Spark, datediff en minutos:
            lag_min = df_out.select((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.lit(row["mx"])))/60.0).first()[0]
            metrics = dict(metrics)
            metrics["freshness_lag_minutes"] = float(lag_min)
    return metrics

def with_extras(metrics: Dict[str, Any], extras: Dict[str, Any]) -> Dict[str, Any]:
    m = dict(metrics)
    m["extras_json"] = json.dumps(extras or {}, ensure_ascii=False)
    return m

# 4) Escritura (idéntica a Bronze, con el nuevo esquema)
def write_metrics(
    spark,
    metrics: Dict[str, Any],
    rows_out: Optional[int] = None,
    status: str = "ok",
    notes: str = "",
) -> None:
    m = dict(metrics)
    if rows_out is not None:
        m["rows_out"] = int(rows_out)
    m["status"] = status
    m["notes"] = notes

    schema = T.StructType([
        T.StructField("dataset",      T.StringType(),   False),
        T.StructField("source_key",   T.StringType(),   False),
        T.StructField("ingest_id",    T.StringType(),   False),
        T.StructField("processed_ts", T.TimestampType(),False),

        T.StructField("rows_in",      T.LongType(),     True),
        T.StructField("rows_out",     T.LongType(),     True),
        T.StructField("corrupt_rows", T.LongType(),     True),

        T.StructField("nulls_json",   T.StringType(),   True),
        T.StructField("columns_json", T.StringType(),   True),

        T.StructField("dup_ratio",            T.DoubleType(),   True),
        T.StructField("unknown_pct",          T.DoubleType(),   True),
        T.StructField("freshness_lag_minutes",T.DoubleType(),   True),
        T.StructField("extras_json",          T.StringType(),   True),

        T.StructField("status",       T.StringType(),   True),
        T.StructField("notes",        T.StringType(),   True),
    ])

    df = spark.createDataFrame([(
        m["dataset"], m["source_key"], m["ingest_id"], datetime.utcnow(),
        m.get("rows_in"), m.get("rows_out"), m.get("corrupt_rows"),
        m.get("nulls_json"), m.get("columns_json"),
        m.get("dup_ratio"), m.get("unknown_pct"), m.get("freshness_lag_minutes"), m.get("extras_json"),
        m.get("status"), m.get("notes"),
    )], schema=schema)

    df.write.format("delta").mode("append").save(SILVER_METRICS_PATH)