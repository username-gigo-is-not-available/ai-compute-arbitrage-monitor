import logging
from itertools import pairwise

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.update.schema import UpdateSchema

logger = logging.getLogger(__name__)


# Makes one Iceberg table look like its declaration: creates its namespace and the table if missing, then syncs
# columns, column order and partition spec, and rewrites files written under an older spec (ADR-022).
def sync_table(catalog: Catalog, table_id: tuple[str, str], schema: Schema, partition_spec: PartitionSpec) -> None:
    namespace, _ = table_id
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table_if_not_exists(table_id, schema=schema, partition_spec=partition_spec)
    with table.update_schema() as update:
        add_missing_columns(update, schema)
        drop_removed_columns(update, table.schema(), schema)
        update_schema_order(update, schema)
    update_partition_spec(table, schema, partition_spec)
    repartition_existing_files(table)


def add_missing_columns(update: UpdateSchema, schema: Schema) -> None:
    update.union_by_name(schema)


def drop_removed_columns(update: UpdateSchema, current: Schema, schema: Schema) -> None:
    removed_columns = {f.name for f in current.fields} - {f.name for f in schema.fields}
    for column in sorted(removed_columns):
        logger.warning(f"Dropping column {column}: not in the declared schema (recoverable by time travel)")
        update.delete_column(column)


def update_schema_order(update: UpdateSchema, schema: Schema) -> None:
    columns = [f.name for f in schema.fields]
    update.move_first(columns[0])
    for previous_column, current_column in pairwise(columns):
        update.move_after(current_column, previous_column)


def partition_fields(partition_spec: PartitionSpec, schema: Schema) -> list[tuple[str, str]]:
    return [(schema.find_column_name(partition_field.source_id), str(partition_field.transform))
            for partition_field in partition_spec.fields]


def update_partition_spec(table: Table, schema: Schema, partition_spec: PartitionSpec) -> None:
    if partition_fields(table.spec(), table.schema()) == partition_fields(partition_spec, schema):
        return
    with table.update_spec() as update:
        for partition_field in table.spec().fields:
            update.remove_field(partition_field.name)
        for partition_field in partition_spec.fields:
            update.add_field(schema.find_column_name(partition_field.source_id), partition_field.transform,
                             partition_field.name)


def repartition_existing_files(table: Table) -> None:
    current_spec_id = table.spec().spec_id
    if all(task.file.spec_id == current_spec_id for task in table.scan().plan_files()):
        return
    logger.info(f"Repartitioning {table.name()} into partition spec {current_spec_id}")
    table.overwrite(table.scan().to_arrow())
