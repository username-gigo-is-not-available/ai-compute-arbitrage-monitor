# Every Spark/Iceberg version pin, side by side. No imports: the DAG files import it too, and Airflow has no pyspark.

# Laptop: pyspark 4.1 (Scala 2.13)
LOCAL_ICEBERG_VERSION = "1.11.0"
LOCAL_SPARK_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{LOCAL_ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-gcp-bundle:{LOCAL_ICEBERG_VERSION}",
)

# Dataproc Serverless runtime 2.2 = Spark 3.5 / Scala 2.13; must match Dockerfile.dataproc's FROM dataproc_2.2.
# Iceberg 1.10.2, and only the shaded gcp-bundle, never plain iceberg-gcp (spike 0D, docs/specs/0d-...md).
DATAPROC_RUNTIME_VERSION = "2.2"
DATAPROC_ICEBERG_VERSION = "1.10.2"
DATAPROC_SPARK_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:{DATAPROC_ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-gcp-bundle:{DATAPROC_ICEBERG_VERSION}",
)
