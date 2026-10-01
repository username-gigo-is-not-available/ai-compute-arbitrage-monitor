from common.enums import ExecutionType
from config.gcp_auth import access_token
from config.lakehouse import GCPLakehouseConfig


_ICEBERG_VERSION = "1.11.0"
_ICEBERG_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{_ICEBERG_VERSION},"
    f"org.apache.iceberg:iceberg-gcp-bundle:{_ICEBERG_VERSION}"
)


def configure_spark(builder, lakehouse: GCPLakehouseConfig, execution_type: ExecutionType):
    catalog_id = lakehouse.catalog_id
    builder = (
        builder
        .config(f"spark.sql.catalog.{catalog_id}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_id}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
    )
    for key, value in lakehouse.catalog_props().items():
        builder = builder.config(f"spark.sql.catalog.{catalog_id}.{key}", value)
    if execution_type == ExecutionType.LOCAL:
        builder = (
            builder
            .config("spark.jars.packages", _ICEBERG_PACKAGES)
            .config(f"spark.sql.catalog.{catalog_id}.token", access_token())
            .config("spark.driver.memory", "4g")
        )
    elif execution_type == ExecutionType.GCP:
        builder = builder.config(
            f"spark.sql.catalog.{catalog_id}.rest.auth.type",
            "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        )
    return builder
