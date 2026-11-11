from typing import ClassVar

from pydantic import BaseModel
from pyiceberg.catalog.rest import RestCatalog

from common.classes import Dataset
from common.enums import DatasetType, DataStageType
from config.auth import GCPAuthentication


class GCPLakehouseConfig(BaseModel):
    REST_ENDPOINT: ClassVar[str] = "https://biglake.googleapis.com/iceberg/v1/restcatalog"

    catalog_id: str
    project_id: str
    warehouse: str

    def catalog_props(self) -> dict:
        return {
            "type": "rest",
            "uri": self.REST_ENDPOINT,
            "warehouse": self.warehouse,
            "header.x-goog-user-project": self.project_id,
        }

    @staticmethod
    def stage_namespace(stage: DataStageType, dataset_type: DatasetType) -> str:
        return f"{stage.value}_{dataset_type.value}"

    def namespace(self, stage: DataStageType, dataset: Dataset) -> str:
        return self.stage_namespace(stage, dataset.dataset_type)

    def table_id(self, stage: DataStageType, dataset: Dataset) -> tuple[str, str]:
        return (self.namespace(stage, dataset), dataset.dataset_name.value)

    def spark_table(self, stage: DataStageType, dataset: Dataset) -> str:
        ns, tbl = self.table_id(stage, dataset)
        return f"{self.catalog_id}.{ns}.{tbl}"

    def open_catalog(self) -> RestCatalog:
        return RestCatalog(
            self.catalog_id,
            **{
                **self.catalog_props(),
                "token": GCPAuthentication().access_token,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )
