import logging
from typing import ClassVar

from pydantic import BaseModel
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.update.schema import UpdateSchema

from common.classes import Dataset
from common.enums import DataStageType, DatasetType
from config.auth import GCPAuthentication

logger = logging.getLogger(__name__)


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
        return RestCatalog(self.catalog_id, **{
            **self.catalog_props(),
            "token": GCPAuthentication().access_token,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        })

    def ensure_namespace(self, catalog: Catalog, stage: DataStageType, dataset: Dataset) -> None:
        catalog.create_namespace_if_not_exists(self.namespace(stage, dataset))

    def ensure_table(self, catalog: Catalog, stage: DataStageType, dataset: Dataset,
                     schema: Schema, partition_spec: PartitionSpec) -> None:
        self.ensure_namespace(catalog, stage, dataset)
        table = catalog.create_table_if_not_exists(self.table_id(stage, dataset),
                                                   schema=schema, partition_spec=partition_spec)
        with table.update_schema() as update:
            self.add_missing_columns(update, schema)
            self.drop_removed_columns(update, table.schema(), schema)
            self.update_schema_order(update, schema)
        self.update_partition_spec(table, schema, partition_spec)
        self.repartition_existing_files(table)

    @staticmethod
    def add_missing_columns(update: UpdateSchema, schema: Schema) -> None:
        update.union_by_name(schema)

    @staticmethod
    def drop_removed_columns(update: UpdateSchema, current: Schema, schema: Schema) -> None:
        removed_columns = {f.name for f in current.fields} - {f.name for f in schema.fields}
        for column in sorted(removed_columns):
            logger.warning(f"Dropping column {column}: not in the declared schema (recoverable by time travel)")
            update.delete_column(column)

    @staticmethod
    def update_schema_order(update: UpdateSchema, schema: Schema) -> None:
        columns = [f.name for f in schema.fields]
        update.move_first(columns[0])
        for previous_column, current_column in zip(columns, columns[1:]):
            update.move_after(current_column, previous_column)

    @staticmethod
    def partition_fields(partition_spec: PartitionSpec, schema: Schema) -> list[tuple[str, str]]:
        return [(schema.find_column_name(partition_field.source_id), str(partition_field.transform))
                for partition_field in partition_spec.fields]

    def update_partition_spec(self, table: Table, schema: Schema, partition_spec: PartitionSpec) -> None:
        if self.partition_fields(table.spec(), table.schema()) == self.partition_fields(partition_spec, schema):
            return
        with table.update_spec() as update:
            for partition_field in table.spec().fields:
                update.remove_field(partition_field.name)
            for partition_field in partition_spec.fields:
                update.add_field(schema.find_column_name(partition_field.source_id), partition_field.transform,
                                 partition_field.name)

    @staticmethod
    def repartition_existing_files(table: Table) -> None:
        current_spec_id = table.spec().spec_id
        if all(task.file.spec_id == current_spec_id for task in table.scan().plan_files()):
            return
        logger.info(f"Repartitioning {table.name()} into partition spec {current_spec_id}")
        table.overwrite(table.scan().to_arrow())
