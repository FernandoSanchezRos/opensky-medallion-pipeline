# spark.Dockerfile
FROM bitnami/spark:3.5

USER root

# ---- Python deps (en el Python de Bitnami) ----
RUN /opt/bitnami/python/bin/python3 -m pip install --no-cache-dir \
      "delta-spark==3.2.*" \
      importlib-metadata

# ---- Usuario 'spark' con UID 50000 para alinear con docker-compose ----
# (así /etc/passwd tendrá la entrada y Hadoop/JAAS no fallará)
RUN useradd -m -u 50000 -s /usr/sbin/nologin spark

# ---- Directorios y permisos ----
RUN mkdir -p /opt/spark/spark-events /opt/spark/.ivy2 /opt/spark/tmp \
    && chown -R 50000:0 /opt/spark

# ---- Entorno útil ----
ENV HOME=/opt/spark \
    PYSPARK_PYTHON=/opt/bitnami/python/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3 \
    # Delta (el runtime ya lo carga, pero dejamos las conf por si no usas tu helper)
    SPARK_USER=spark \
    HADOOP_USER_NAME=spark

# (Opcional) Defaults de Spark si no los pasas por código/CLI
# COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Cambiamos al usuario final (coincide con docker-compose)
USER 50000
