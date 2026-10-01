from typing import ClassVar

from pydantic import BaseModel
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema

from common.classes import Dataset
from common.enums import DataStageType, DatasetType
from config.gcp_auth import access_token


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
        # Flat <stage>_<dataset_type>: BigLake rejects nested namespaces (ADR-016).
        return f"{stage.value}_{dataset_type.value}"

    def namespace(self, stage: DataStageType, dataset: Dataset) -> str:
        return self.stage_namespace(stage, dataset.dataset_type)

    def table_id(self, stage: DataStageType, dataset: Dataset) -> tuple[str, str]:
        return (self.namespace(stage, dataset), dataset.dataset_name.value)

    def spark_table(self, stage: DataStageType, dataset: Dataset) -> str:
        ns, tbl = self.table_id(stage, dataset)
        return f"{self.catalog_id}.{ns}.{tbl}"

    def open_catalog(self) -> RestCatalog:
        # pyiceberg REST client has no Google ADC integration; pass a fresh Bearer token explicitly
        return RestCatalog(self.catalog_id, **{
            **self.catalog_props(),
            "token": access_token(),
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        })

    def ensure_namespace(self, catalog: Catalog, stage: DataStageType, dataset: Dataset) -> None:
        try:
            catalog.create_namespace(self.namespace(stage, dataset))
        except NamespaceAlreadyExistsError:
            pass

    def ensure_table(self, catalog: Catalog, stage: DataStageType, dataset: Dataset,
                     schema: Schema, partition_spec: PartitionSpec) -> None:
        self.ensure_namespace(catalog, stage, dataset)
        table_id = self.table_id(stage, dataset)
        if not catalog.table_exists(table_id):
            catalog.create_table(table_id, schema=schema, partition_spec=partition_spec)
