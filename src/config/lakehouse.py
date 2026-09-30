from typing import ClassVar

import pyarrow as pa
from pydantic import BaseModel

from common.classes import Dataset
from common.enums import DataStageType, DatasetType, ExecutionType


class GCPLakehouseConfig(BaseModel):
    REST_ENDPOINT: ClassVar[str] = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
    # Spark SQL-safe alias for the catalog: the real catalog_id contains hyphens
    # which break Spark SQL identifier parsing (e.g. `ai-compute-... .ns.table`).
    _SPARK_ALIAS: ClassVar[str] = "lake"

    catalog_id: str
    project_id: str
    warehouse: str

    def namespace(self, stage: DataStageType, dataset: Dataset) -> str:
        return f"{stage.value}_{dataset.dataset_type.value}"

    def table_id(self, stage: DataStageType, dataset: Dataset) -> tuple[str, str]:
        return (self.namespace(stage, dataset), dataset.dataset_name.value)

    def spark_table(self, stage: DataStageType, dataset: Dataset) -> str:
        return f"{self._SPARK_ALIAS}.{self.namespace(stage, dataset)}.{dataset.dataset_name.value}"

    def open_catalog(self):
        # pyiceberg REST client has no Google ADC integration; pass a fresh Bearer token explicitly
        from google.auth import default as gcp_default
        from google.auth.transport.requests import Request as GcpRequest
        from pyiceberg.catalog import load_catalog

        creds, _ = gcp_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(GcpRequest())

        return load_catalog(
            self.catalog_id,
            **{
                "type": "rest",
                "uri": self.REST_ENDPOINT,
                "warehouse": self.warehouse,
                "header.x-goog-user-project": self.project_id,
                "token": creds.token,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )

    def write(self, catalog, arrow_table: pa.Table, stage: DataStageType, dataset: Dataset) -> None:
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        from pyiceberg.io.pyarrow import pyarrow_to_schema

        namespace = self.namespace(stage, dataset)
        table_id = self.table_id(stage, dataset)

        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

        if catalog.table_exists(table_id):
            iceberg_table = catalog.load_table(table_id)
        else:
            schema = pyarrow_to_schema(arrow_table.schema)
            iceberg_table = catalog.create_table(
                table_id,
                schema=schema,
                partition_spec=self.partition_spec(dataset.dataset_type, schema),
            )

        if dataset.dataset_type == DatasetType.SOURCES:
            iceberg_table.append(arrow_table)
        else:
            iceberg_table.overwrite(arrow_table)

    def partition_spec(self, dataset_type: DatasetType, pyiceberg_schema):
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import HourTransform, IdentityTransform

        if dataset_type == DatasetType.SOURCES:
            field_id = pyiceberg_schema.find_field("ingested_at").field_id
            return PartitionSpec(
                PartitionField(source_id=field_id, field_id=1000, transform=HourTransform(), name="ingested_at_hour")
            )
        field_id = pyiceberg_schema.find_field("valid_from").field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=IdentityTransform(), name="valid_from")
        )

    def configure_spark(self, builder, execution_type: ExecutionType):
        c = self._SPARK_ALIAS
        builder = (
            builder
            .config(f"spark.sql.catalog.{c}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{c}.type", "rest")
            .config(f"spark.sql.catalog.{c}.uri", self.REST_ENDPOINT)
            .config(f"spark.sql.catalog.{c}.warehouse", self.warehouse)
            .config(f"spark.sql.catalog.{c}.header.x-goog-user-project", self.project_id)
            .config(f"spark.sql.catalog.{c}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
        )
        if execution_type == ExecutionType.LOCAL:
            from google.auth import default as gcp_default
            from google.auth.transport.requests import Request as GcpRequest
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
