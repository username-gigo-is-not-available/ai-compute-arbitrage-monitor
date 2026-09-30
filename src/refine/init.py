from pyspark.sql import SparkSession

from common.enums import ExecutionType
from config.lakehouse import REST_ENDPOINT, SPARK_CATALOG_ALIAS
from config.loader import ConfigLoader


def initialize_spark() -> SparkSession:
    loader = ConfigLoader()
    execution_type = loader.get_execution_type()
    lakehouse = loader.get_lakehouse()
    c = SPARK_CATALOG_ALIAS

    builder = (
        SparkSession.builder
        .appName("ai-compute-arbitrage-monitor")
        .config(f"spark.sql.catalog.{c}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{c}.type", "rest")
        .config(f"spark.sql.catalog.{c}.uri", REST_ENDPOINT)
        .config(f"spark.sql.catalog.{c}.warehouse", lakehouse.warehouse)
        .config(f"spark.sql.catalog.{c}.header.x-goog-user-project", lakehouse.project_id)
        .config(f"spark.sql.catalog.{c}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
    )

    if execution_type == ExecutionType.LOCAL:
        import google.auth
        import google.auth.transport.requests

        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(google.auth.transport.requests.Request())

        builder = (
            builder
            .config(f"spark.sql.catalog.{c}.token", creds.token)
            .config("spark.driver.memory", "4g")
        )
    elif execution_type == ExecutionType.GCP:
        builder = builder.config(
            f"spark.sql.catalog.{c}.rest.auth.type",
            "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        )
    else:
        raise ValueError(f"Unknown execution type: {execution_type}")

    return builder.getOrCreate()
