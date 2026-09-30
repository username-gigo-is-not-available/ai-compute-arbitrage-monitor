from pydantic import BaseModel

from common.classes import Dataset
from common.enums import DataStageType, DatasetType

REST_ENDPOINT = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
SPARK_CATALOG_ALIAS = "lake"


class GCPLakehouseConfig(BaseModel):
    catalog_id: str
    project_id: str
    warehouse: str

    def namespace(self, stage: DataStageType, dataset: Dataset) -> str:
        return f"{stage.value}_{dataset.dataset_type.value}"

    def table_id(self, stage: DataStageType, dataset: Dataset) -> tuple[str, str]:
        return (self.namespace(stage, dataset), dataset.dataset_name.value)

    def spark_table(self, stage: DataStageType, dataset: Dataset) -> str:
        return f"{SPARK_CATALOG_ALIAS}.{self.namespace(stage, dataset)}.{dataset.dataset_name.value}"

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
                "uri": REST_ENDPOINT,
                "warehouse": self.warehouse,
                "header.x-goog-user-project": self.project_id,
                "token": creds.token,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )

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
