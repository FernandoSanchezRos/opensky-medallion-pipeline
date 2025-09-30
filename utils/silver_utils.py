# utils/silver_utils.py
from pyspark.sql import DataFrame, functions as F, Window as W
from typing import Optional, Iterable, Sequence, Dict
import os

DEFAULT_SILVER_VERSION = os.getenv("SILVER_VERSION", "v1.0.0")
DEFAULT_SILVER_RUN_ID  = os.getenv("SILVER_RUN_ID", "manual")
DEFAULT_SILVER_JOB     = os.getenv("SILVER_JOB", "unknown")

# --- METADATA ---

def add_silver_metadata(
    df: DataFrame,
    source_name: str,
    silver_version: Optional[str] = None,
    run_id: Optional[str] = None,
    job_name: Optional[str] = None,
) -> DataFrame:
    """
    Añade metadatos de Silver sin romper lineage de Bronze.
    """
    return (df
        .withColumn("_silver_version", F.lit(silver_version or DEFAULT_SILVER_VERSION))
        .withColumn("_silver_ts",      F.current_timestamp())
        .withColumn("_silver_run_id",  F.lit(run_id or DEFAULT_SILVER_RUN_ID))
        .withColumn("_silver_job",     F.lit(job_name or DEFAULT_SILVER_JOB))
        .withColumn("_source",         F.lit(source_name))
    )

def select_allowlist(df: DataFrame, keep_cols: Iterable[str]) -> DataFrame:
    keep = [c for c in keep_cols if c in df.columns]
    return df.select(*keep)

# --- NORMALIZADORES ÚTILES (por si no los tienes ya) ---

def normalize_str(col: str) -> F.Column:
    return F.upper(F.trim(F.col(col)))

def to_utc_ts(col: str) -> F.Column:
    # epoch (s o ms) -> timestamp UTC
    c = F.col(col)
    c_s = F.when(c > F.lit(10_000_000_000), (c / 1000).cast("long")).otherwise(c.cast("long"))
    return F.to_utc_timestamp(F.to_timestamp(c_s), "UTC")

def add_calendar(df: DataFrame, ts_col: str, prefix: str) -> DataFrame:
    ts = F.col(ts_col)
    return (df
        .withColumn(f"{prefix}_date_utc", F.to_date(ts))
        .withColumn(f"{prefix}_hour",     F.hour(ts).cast("tinyint"))
        .withColumn(f"{prefix}_dow",      F.dayofweek(ts).cast("tinyint"))
        .withColumn(f"{prefix}_week",     F.weekofyear(ts))
        .withColumn(f"{prefix}_month",    F.date_format(ts, "yyyy-MM"))
    )

def dedupe_by_keys(df: DataFrame, keys: Sequence[str], order_col: str = "_ingest_ts") -> DataFrame:
    w = W.partitionBy(*[F.col(k) for k in keys]).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn")==1).drop("_rn")

# --- ESCRITURA ---

def write_delta_append(
    df: DataFrame,
    dest: str,
    partition_cols: Optional[Iterable[str]] = None,
    merge_schema: bool = True,
) -> None:
    """
    Append simple (útil para logs/metricas). Evita particionar por _silver_version.
    """
    writer = (df.write.format("delta")
                .mode("append")
                .option("mergeSchema", str(merge_schema).lower()))
    parts = list(partition_cols or [])
    if parts:
        writer = writer.partitionBy(*parts)
    writer.save(dest)

def merge_upsert(
    spark,
    df_source: DataFrame,
    dest_path: str,
    keys_expr: str,
    partition_cols: Optional[Iterable[str]] = None,
    set_when_matched: Optional[Dict[str, str]] = None,
) -> None:
    """
    MERGE INTO por clave natural (recomendado en Silver).
    keys_expr: condición SQL para el ON (ej: "t.flight_id = s.flight_id")
    """
    from delta.tables import DeltaTable
    try:
        delta = DeltaTable.forPath(spark, dest_path)
        m = delta.alias("t").merge(df_source.alias("s"), keys_expr)
        m = m.whenMatchedUpdate(set=set_when_matched or {c: f"s.{c}" for c in df_source.columns})
        m = m.whenNotMatchedInsertAll()
        m.execute()
    except Exception:
        # Primera vez: escribe tabla
        writer = (df_source.write.format("delta").mode("overwrite"))
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(dest_path)
