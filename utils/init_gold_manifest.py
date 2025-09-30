from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
import glob, hashlib, os

# Tabla Delta del manifiesto de GOLD
META_PATH = "/opt/spark/lakehouse/meta/gold_runs"

# Esquema (análogo a Silver) + gold_version
MANIFEST_SCHEMA = T.StructType([
    T.StructField("dataset",      T.StringType(),   False),
    T.StructField("source_file",  T.StringType(),   False),
    T.StructField("ingest_id",    T.StringType(),   False),   # usa aquí tu run_id de Gold
    T.StructField("processed_ts", T.TimestampType(),False),
    T.StructField("status",       T.StringType(),   False),   # 'processing' | 'done' | 'failed'
    T.StructField("file_hash",    T.StringType(),   True),    # opcional (para snapshots)
    T.StructField("gold_version", T.StringType(),   True),    # versión del proceso Gold
])

# -------- helpers internos --------
def _as_delta(spark) -> DeltaTable:
    try:
        return DeltaTable.forPath(spark, META_PATH)
    except Exception:
        ensure_manifest(spark)
        return DeltaTable.forPath(spark, META_PATH)

def _paths_to_df(spark, files, dataset: str, ingest_id: str, status: str,
                 file_hash: str | None = None, gold_version: str | None = None):
    df = (spark.createDataFrame(files, T.StringType()).toDF("source_file")
          .withColumn("dataset",      F.lit(dataset))
          .withColumn("ingest_id",    F.lit(ingest_id))
          .withColumn("processed_ts", F.current_timestamp())
          .withColumn("status",       F.lit(status))
          .withColumn("file_hash",    F.lit(file_hash).cast(T.StringType()))
          .withColumn("gold_version", F.lit(gold_version).cast(T.StringType())))
    return df.select("dataset","source_file","ingest_id","processed_ts","status","file_hash","gold_version")

# -------- API pública --------
def ensure_manifest(spark):
    try:
        spark.read.format("delta").load(META_PATH).limit(1).collect()
        print("[INFO] Gold manifest ya existe.")
    except Exception:
        print("[INFO] Creando Gold manifest vacío...")
        empty_df = spark.createDataFrame([], MANIFEST_SCHEMA)
        empty_df.write.format("delta").mode("overwrite").save(META_PATH)

def needs_processing(spark, dataset: str, source_file: str, gold_version: str) -> bool:
    """
    True si NO hay registro 'done' para (dataset, source_file) con esa misma gold_version.
    Re-procesa si es nuevo o la versión no coincide.
    """
    try:
        df = (spark.read.format("delta").load(META_PATH)
              .filter( (F.col("dataset")==dataset) &
                       (F.col("source_file")==source_file) &
                       (F.col("status")=="done") &
                       (F.col("gold_version")==gold_version) ))
        return df.limit(1).count() == 0
    except Exception:
        return True

def discover_new_files(spark, input_root: str, dataset: str, pattern: str = "date=*/*.parquet"):
    """
    Igual que en Silver: devuelve paths de input_root que NO están 'done' (cualquier versión).
    Útil si tu input de Gold son ficheros/particiones (p.ej., exports o snapshots).
    """
    path_pattern = os.path.join(input_root.rstrip("/"), pattern)
    candidates = glob.glob(path_pattern)
    if not candidates:
        return []

    df_candidates = spark.createDataFrame(candidates, T.StringType()).toDF("source_file")
    try:
        df_done = (spark.read.format("delta").load(META_PATH)
                   .filter( (F.col("dataset")==dataset) & (F.col("status")=="done") )
                   .select("source_file").distinct())
        df_new = df_candidates.join(df_done, on="source_file", how="left_anti")
        return [r["source_file"] for r in df_new.collect()]
    except Exception:
        return candidates

def update_manifest(spark, files, dataset: str, ingest_id: str,
                    status: str = "done", file_hash: str | None = None,
                    gold_version: str | None = None):
    """
    Upsert idempotente. Clave lógica: (dataset, source_file).
    Si re-procesas con nueva gold_version, la fila se actualiza con la versión nueva.
    """
    if not files:
        return
    delta = _as_delta(spark)
    df_new = _paths_to_df(spark, files, dataset, ingest_id, status, file_hash, gold_version)

    (delta.alias("t").merge(
        df_new.alias("s"),
        "t.dataset = s.dataset AND t.source_file = s.source_file"
    ).whenMatchedUpdate(
        condition="t.status <> s.status OR t.file_hash <> s.file_hash OR t.ingest_id <> s.ingest_id OR t.gold_version <> s.gold_version",
        set={
            "ingest_id":    "s.ingest_id",
            "processed_ts": "s.processed_ts",
            "status":       "s.status",
            "file_hash":    "s.file_hash",
            "gold_version": "s.gold_version",
        }
    ).whenNotMatchedInsertAll()
     .execute())

# -------- utilidades hash (snapshots) --------
def _compute_local_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def snapshot_needs_processing(spark, dataset: str, file_path: str, gold_version: str) -> bool:
    """
    True si (a) el MD5 cambió o (b) no existe 'done' con esta gold_version.
    """
    file_hash = _compute_local_md5(file_path)
    try:
        df = (spark.read.format("delta").load(META_PATH)
              .filter( (F.col("dataset")==dataset) &
                       (F.col("source_file")==file_path) &
                       (F.col("status")=="done") ))
        last = df.orderBy(F.col("processed_ts").desc()).limit(1).collect()
        if not last:
            return True
        same_hash = ( (last[0]["file_hash"] or "") == file_hash )
        same_ver  = ( (last[0]["gold_version"] or "") == gold_version )
        return not (same_hash and same_ver)
    except Exception:
        return True

def compute_file_hash(path: str) -> str:
    return _compute_local_md5(path)
