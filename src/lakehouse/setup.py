from common.enums import DatasetName, DataStageType
from config.datasets import DATASETS
from config.loader import ConfigLoader
from ingest.tables import BRONZE_TABLES
from lakehouse.sync import sync_table
from refine.tables import SILVER_TABLES


# The DDL step: makes one dataset's Bronze and Silver tables match their declarations (ADR-022), before ingest and
# refine load anything. Imports pyspark (through the Silver schemas), so the DAG starts it as a command and never
# imports it: Composer does not have pyspark.
def run_setup(dataset: str) -> None:
    name = DatasetName(dataset)
    declaration = DATASETS[name]
    loader = ConfigLoader()
    if not declaration.config(loader).enabled:
        return
    lakehouse = loader.get_lakehouse()
    catalog = lakehouse.open_catalog()
    bronze, silver = BRONZE_TABLES[name], SILVER_TABLES[name]
    sync_table(catalog, lakehouse.table_id(DataStageType.BRONZE, declaration.dataset),
               schema=bronze.schema, partition_spec=bronze.partition_spec())
    sync_table(catalog, lakehouse.table_id(DataStageType.SILVER, declaration.dataset),
               schema=silver.iceberg_schema(), partition_spec=silver.partition_spec())
