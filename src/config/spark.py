from google.auth import default as gcp_default
from google.auth.transport.requests import Request as GcpRequest

from common.enums import ExecutionType
from config.lakehouse import GCPLakehouseConfig


def configure_spark(builder, lakehouse: GCPLakehouseConfig, execution_type: ExecutionType):
    c = lakehouse.spark_alias
    builder = (
        builder
        .config(f"spark.sql.catalog.{c}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{c}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
    )
    for key, value in lakehouse.catalog_props().items():
        builder = builder.config(f"spark.sql.catalog.{c}.{key}", value)
    if execution_type == ExecutionType.LOCAL:
        creds, _ = gcp_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(GcpRequest())
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
    return builder
