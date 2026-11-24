# syntax=docker/dockerfile:1.7
FROM apache/spark:3.5.9-java17-python3

USER root

# Delta Lake 3.3.2 (Spark 3.5, Scala 2.12) and the PostgreSQL JDBC driver, checked by sha256.
ADD --chmod=644 --checksum=sha256:4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85 \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36 \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:157963d60ae66d607e09466e8c0cdf8087e9cb20d0159899ffca96bca2528460 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar /opt/spark/jars/

# delta-spark Python API only (pyspark ships with the base image); pytest for in-container tests.
RUN pip3 install --no-cache-dir --no-deps delta-spark==3.3.2 \
    && pip3 install --no-cache-dir pytest==8.3.5 pytest-cov==6.1.1 \
    && mkdir -p /data/delta /app \
    && chown spark:spark /data/delta /app

ENV PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    DELTA_ROOT=/data/delta \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY --chown=spark:spark spark/ /app/spark/

USER spark
EXPOSE 8000
CMD ["python3", "-m", "spark.trigger"]
