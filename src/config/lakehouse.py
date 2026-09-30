from typing import ClassVar

from google.auth import default as gcp_default
from google.auth.transport.requests import Request as GcpRequest
from pydantic import BaseModel
from pyiceberg.catalog import load_catalog

from common.classes import Dataset
from common.enums import DataStageType


class GCPLakehouseConfig(BaseModel):
    REST_ENDPOINT: ClassVar[str] = "https://biglake.googleapis.com/iceberg/v1/restcatalog"

    catalog_id: str
    project_id: str
    warehouse: str

    @property
    def spark_alias(self) -> str:
        return self.catalog_id.replace("-", "_")

    def catalog_props(self) -> dict:
        return {
            "type": "rest",
            "uri": self.REST_ENDPOINT,
            "warehouse": self.warehouse,
            "header.x-goog-user-project": self.project_id,
        }

    def namespace(self, stage: DataStageType, dataset: Dataset) -> str:
        return f"{stage.value}_{dataset.dataset_type.value}"

    def table_id(self, stage: DataStageType, dataset: Dataset) -> tuple[str, str]:
        return (self.namespace(stage, dataset), dataset.dataset_name.value)

    def spark_table(self, stage: DataStageType, dataset: Dataset) -> str:
        ns, tbl = self.table_id(stage, dataset)
        return f"{self.spark_alias}.{ns}.{tbl}"

    def open_catalog(self):
        # pyiceberg REST client has no Google ADC integration; pass a fresh Bearer token explicitly
        creds, _ = gcp_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(GcpRequest())
        return load_catalog(self.catalog_id, **{
            **self.catalog_props(),
            "token": creds.token,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        })

