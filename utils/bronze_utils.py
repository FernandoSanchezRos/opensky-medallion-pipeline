from pyspark.sql import DataFrame, functions as F
from typing import Optional, Iterable
import uuid

# ============ 1) Metadatos estándar ============
def add_common_metadata(df: DataFrame, ingest_id: Optional[str] = None) -> DataFrame:
    """
    Añade columnas estándar de ingesta BRONZE:
      - _ingest_id (STRING)
      - _ingest_ts (TIMESTAMP)
      - _source_file (STRING)
      - _ingestion_date (DATE)
      - download_ts (TIMESTAMP)  # timestamp de descarga inferido del filename
      - download_date (DATE)     # fecha de descarga para particionar
    """
    if ingest_id is None:
        ingest_id = str(uuid.uuid4())

    src = F.input_file_name()
    ingest_ts = F.current_timestamp()  # único valor para toda la selección
    ingest_date = F.current_date()

    # Fecha del directorio: .../date=YYYY-MM-DD/...
    file_date_str = F.regexp_extract(src, r"(?:^|[\\/])date=(\d{4}-\d{2}-\d{2})(?:[\\/]|$)", 1)
    file_date = F.to_date(file_date_str)

    # Timestamp exacto en el nombre: ..._YYYYMMDDThhmmssZ...
    dl_str = F.regexp_extract(src, r"_(\d{8}T\d{6})Z", 1)      # "20250919T082017"
    dl_ts = F.to_timestamp(dl_str, "yyyyMMdd'T'HHmmss")        # TIMESTAMP
    dl_date = F.to_date(dl_ts)                                  # DATE

    return df.select(
        "*",
        F.lit(ingest_id).alias("_ingest_id"),
        ingest_ts.alias("_ingest_ts"),
        src.alias("_source_file"),
        ingest_date.alias("_ingestion_date"),
        dl_ts.alias("download_ts"),
        # plan A: dl_date | plan B: file_date | plan C: date(_ingest_ts)
        F.coalesce(dl_date, file_date, F.to_date(ingest_ts)).alias("download_date"),
    )


def write_delta(
    df: DataFrame,
    dest: str,
    partition_cols: Optional[Iterable[str]] = None,
    mode: str = "append",
    merge_schema: bool = True,
) -> None:
    w = df.write.format("delta").mode(mode).option("mergeSchema", str(merge_schema).lower())
    if partition_cols:
        w = w.partitionBy(list(partition_cols))
    w.save(dest)
