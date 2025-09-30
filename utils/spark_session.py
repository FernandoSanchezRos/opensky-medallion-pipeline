from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession configured for Delta Lake.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # Recursos (ajusta según tu cluster/local)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        # Shuffle
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        # Serialización / compresión
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Overwrite de particiones
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Event log (para history server)
        .config("spark.eventLog.enabled", "false")
        .config("spark.eventLog.dir", "file:/opt/spark/spark-events")
        # Consola más amigable
        .config("spark.ui.showConsoleProgress", "true")
        # Donde cachea Ivy los jars descargados
        .config("spark.jars.ivy", "/opt/spark/.ivy2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.sql.warehouse.dir", "file:/opt/spark/lakehouse/warehouse")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark