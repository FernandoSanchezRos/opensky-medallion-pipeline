# utils/init_bronze_manifest.py
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
import glob, hashlib, os

# Tabla Delta única para el manifiesto (global para todos los datasets)
META_PATH = "/opt/spark/lakehouse/meta/bronze_processed_files"

# Esquema base (file_hash es opcional; útil para snapshots CSV)
MANIFEST_SCHEMA = T.StructType([
    T.StructField("dataset",      T.StringType(),  False),
    T.StructField("source_file",  T.StringType(),  False),
    T.StructField("ingest_id",    T.StringType(),  False),
    T.StructField("processed_ts", T.TimestampType(), False),
    T.StructField("status",       T.StringType(),  False),  # 'processing' | 'done' | 'failed'
    T.StructField("file_hash",    T.StringType(),  True),   # opcional (para snapshots)
])

# -------- helpers internos --------
def _as_delta(spark) -> DeltaTable:
    try:
        return DeltaTable.forPath(spark, META_PATH)
    except Exception:
        ensure_manifest(spark)
        return DeltaTable.forPath(spark, META_PATH)

def _paths_to_df(spark, files, dataset: str, ingest_id: str, status: str, file_hash: str | None = None):
    df = spark.createDataFrame(files, T.StringType()).toDF("source_file") \
        .withColumn("dataset",      F.lit(dataset)) \
        .withColumn("ingest_id",    F.lit(ingest_id)) \
        .withColumn("processed_ts", F.current_timestamp()) \
        .withColumn("status",       F.lit(status)) \
        .withColumn("file_hash",    F.lit(file_hash).cast(T.StringType()))
    # orden de columnas consistente
    return df.select("dataset", "source_file", "ingest_id", "processed_ts", "status", "file_hash")

def _compute_local_md5(path: str) -> str:
    """Hash MD5 de un archivo local (para snapshots CSV)."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

# -------- API pública que usas en tus jobs --------
def ensure_manifest(spark):
    """Crea la tabla Delta del manifiesto si no existe."""
    try:
        spark.read.format("delta").load(META_PATH).limit(1).collect()
        print("[INFO] Manifiesto ya existe.")
    except Exception:
        print("[INFO] Creando manifiesto vacío...")
        empty_df = spark.createDataFrame([], MANIFEST_SCHEMA)
        empty_df.write.format("delta").mode("overwrite").save(META_PATH)

def discover_new_files(spark, input_root: str, dataset: str, pattern: str = "date=*/*.json.gz"):
    """
    Devuelve la lista de ficheros dentro de input_root que aún NO están 'done' en el manifiesto.
    Usa anti-join en Spark (evita colectar grandes listados al driver).
    """
    path_pattern = os.path.join(input_root.rstrip("/"), pattern)
    candidates = glob.glob(path_pattern)
    if not candidates:
        return []

    df_candidates = spark.createDataFrame(candidates, T.StringType()).toDF("source_file")

    try:
        df_done = (
            spark.read.format("delta").load(META_PATH)
                .filter((F.col("dataset") == F.lit(dataset)) & (F.col("status") == F.lit("done")))
                .select("source_file").distinct()
        )
        df_new = df_candidates.join(df_done, on="source_file", how="left_anti")
        return [r["source_file"] for r in df_new.collect()]
    except Exception:
        # si el manifiesto no existe/vacío → todos son candidatos
        return candidates

def update_manifest(spark, files, dataset: str, ingest_id: str, status: str = "done", file_hash: str | None = None):
    """
    Inserta/actualiza el manifiesto de forma idempotente.
    - Para incrementales (states/flights) llama con status='done' y sin file_hash.
    - Para snapshots (CSV) puedes pasar file_hash (MD5) del archivo.
    """
    if not files:
        return

    delta = _as_delta(spark)
    df_new = _paths_to_df(spark, files, dataset, ingest_id, status, file_hash)

    # Clave lógica: (dataset, source_file)
    # Si ya existe como 'done', no toca; si existe con otro status, actualiza a 'done' y refresca ingest_id/processed_ts/hash.
    delta.alias("t").merge(
        df_new.alias("s"),
        "t.dataset = s.dataset AND t.source_file = s.source_file"
    ).whenMatchedUpdate(
        condition="t.status <> s.status OR t.file_hash <> s.file_hash",
        set={
            "ingest_id": "s.ingest_id",
            "processed_ts": "s.processed_ts",
            "status": "s.status",
            "file_hash": "s.file_hash"
        }
    ).whenNotMatchedInsertAll().execute()

# -------- utilidades para snapshots CSV (opcional) --------
def snapshot_needs_processing(spark, dataset: str, file_path: str) -> bool:
    """
    Para datasets de referencia (CSV) decide si hay que reprocesar.
    Regla: si el hash MD5 cambió (o no hay registro previo), hay que procesar.
    """
    file_hash = _compute_local_md5(file_path)
    try:
        df = (spark.read.format("delta").load(META_PATH)
              .filter((F.col("dataset") == dataset) & (F.col("source_file") == file_path) & (F.col("status") == "done"))
              .orderBy(F.col("processed_ts").desc()))
        last = df.select("file_hash").limit(1).collect()
        if not last:
            return True
        return (last[0]["file_hash"] or "") != file_hash
    except Exception:
        return True

def compute_file_hash(path: str) -> str:
    """Expuesto por si quieres calcular el hash fuera y pasarlo a update_manifest."""
    return _compute_local_md5(path)
