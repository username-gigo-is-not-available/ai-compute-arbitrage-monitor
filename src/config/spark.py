from pyspark.sql import SparkSession

from common.enums import ExecutionType
from config.auth import GCPAuthentication
from config.lakehouse import GCPLakehouseConfig
from config.versions import LOCAL_SPARK_PACKAGES


def configure_spark(
    builder: SparkSession.Builder, lakehouse: GCPLakehouseConfig, execution_type: ExecutionType
) -> SparkSession.Builder:
    catalog_id = lakehouse.catalog_id
    builder = builder.config(f"spark.sql.catalog.{catalog_id}", "org.apache.iceberg.spark.SparkCatalog").config(
        f"spark.sql.catalog.{catalog_id}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO"
    )
    for key, value in lakehouse.catalog_props().items():
        builder = builder.config(f"spark.sql.catalog.{catalog_id}.{key}", value)
    if execution_type == ExecutionType.LOCAL:
        builder = (
            builder.config("spark.jars.packages", ",".join(LOCAL_SPARK_PACKAGES))
            .config(f"spark.sql.catalog.{catalog_id}.token", GCPAuthentication().access_token)
            .config("spark.driver.memory", "4g")
        )
    elif execution_type == ExecutionType.GCP:
        builder = builder.config(
            f"spark.sql.catalog.{catalog_id}.rest.auth.type",
            "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        )
    return builder
