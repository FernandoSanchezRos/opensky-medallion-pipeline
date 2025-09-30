from pyspark.sql import DataFrame, functions as F, types as T
from typing import Iterable, Optional, Dict, Any
import os, json
from datetime import datetime

BRONZE_METRICS_PATH = os.getenv("BRONZE_METRICS_PATH", "/opt/spark/lakehouse/meta/bronze_metrics")

# 1) Tabla de métricas (genérica y pequeña)
def ensure_bronze_metrics_table(spark) -> None:
    schema = T.StructType([
        T.StructField("dataset",      T.StringType(),   False),
        T.StructField("source_file",  T.StringType(),   False),
        T.StructField("ingest_id",    T.StringType(),   False),
        T.StructField("processed_ts", T.TimestampType(),False),

        T.StructField("rows_in",      T.LongType(),     True),
        T.StructField("rows_out",     T.LongType(),     True),
        T.StructField("corrupt_rows", T.LongType(),     True),

        T.StructField("nulls_json",   T.StringType(),   True),  # {"col": count, ...}
        T.StructField("columns_json", T.StringType(),   True),  # [{"name":..., "type":...}, ...]
        T.StructField("status",       T.StringType(),   True),  # ok|warn|fail
        T.StructField("notes",        T.StringType(),   True),
    ])
    try:
        spark.read.format("delta").load(BRONZE_METRICS_PATH).limit(1).collect()
        print(f"[INFO] bronze_metrics ya existe en {BRONZE_METRICS_PATH}")
    except Exception:
        print(f"[INFO] bronze_metrics ya existe en {BRONZE_METRICS_PATH}")
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(BRONZE_METRICS_PATH)

# 2) Captura métrica simple y común
def capture_metrics(
    df: DataFrame,
    dataset: str,
    source_file: str,
    ingest_id: str,
    exclude_cols: Optional[Iterable[str]] = None,
    rows_in: Optional[int] = None,
) -> Dict[str, Any]:
    d = df.cache()
    rows_in = rows_in if rows_in is not None else d.count()

    # corrupt rows (si existe la columna)
    corrupt = d.filter(F.col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in d.columns else 0

    # nulls por columna (excluyendo las que indiques, p.ej. metadatos)
    excl = set(exclude_cols or [])
    cols_for_nulls = [c for c in d.columns if c not in excl]
    if cols_for_nulls:
        agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).cast("long").alias(c) for c in cols_for_nulls]
        row = d.agg(*agg_exprs).first()
        nulls_map = {c: int(row[c] or 0) for c in cols_for_nulls}
    else:
        nulls_map = {}

    # snapshot de esquema
    cols_json = json.dumps(
        [{"name": f.name, "type": f.dataType.simpleString()} for f in d.schema.fields],
        ensure_ascii=False
    )

    d.unpersist()

    return {
        "dataset": dataset,
        "source_file": source_file,
        "ingest_id": ingest_id,
        "processed_ts": None,           # se rellena al escribir
        "rows_in": int(rows_in),
        "rows_out": None,               # se rellena al escribir
        "corrupt_rows": int(corrupt),
        "nulls_json": json.dumps(nulls_map, ensure_ascii=False),
        "columns_json": cols_json,
        "status": None,
        "notes": None,
    }

# 3) Escritura de la fila de métricas
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
        T.StructField("source_file",  T.StringType(),   False),
        T.StructField("ingest_id",    T.StringType(),   False),
        T.StructField("processed_ts", T.TimestampType(),False),

        T.StructField("rows_in",      T.LongType(),     True),
        T.StructField("rows_out",     T.LongType(),     True),
        T.StructField("corrupt_rows", T.LongType(),     True),

        T.StructField("nulls_json",   T.StringType(),   True),
        T.StructField("columns_json", T.StringType(),   True),
        T.StructField("status",       T.StringType(),   True),
        T.StructField("notes",        T.StringType(),   True),
    ])

    df = spark.createDataFrame([(
        m["dataset"], m["source_file"], m["ingest_id"], datetime.utcnow(),
        m.get("rows_in"), m.get("rows_out"), m.get("corrupt_rows"),
        m.get("nulls_json"), m.get("columns_json"), m.get("status"), m.get("notes"),
    )], schema=schema)

    df.write.format("delta").mode("append").save(BRONZE_METRICS_PATH)
