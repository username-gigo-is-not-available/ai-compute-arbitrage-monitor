import os
import sys
from collections.abc import Callable, Iterator

import pyarrow as pa
import pytest
from fakes import FakeCatalog
from pyspark.sql import SparkSession

from common.enums import DataStageType
from config.lakehouse import GCPLakehouseConfig
from ingest.base import Ingestor
from ingest.tables import BRONZE_TABLES
from lakehouse.sync import sync_table


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # Spark's Python workers must run this interpreter, not whatever `python` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


@pytest.fixture
def catalog() -> Iterator[FakeCatalog]:
    catalog = FakeCatalog()
    yield catalog
    catalog.tmp.cleanup()


@pytest.fixture
def bronze(monkeypatch, catalog) -> Callable[[Ingestor], pa.Table]:
    """Runs an ingestor into its real declared Bronze table, kept by the fake catalog; returns the table's rows."""
    monkeypatch.setattr(GCPLakehouseConfig, "open_catalog", lambda self: catalog)

    def run(ingestor: Ingestor) -> pa.Table:
        declared = BRONZE_TABLES[ingestor.dataset.dataset_name]
        table_id = ingestor.lakehouse_config.table_id(DataStageType.BRONZE, ingestor.dataset)
        # as lakehouse.setup does before every ingest
        sync_table(catalog, table_id, schema=declared.schema, partition_spec=declared.partition_spec())
        ingestor.bronze_table = declared
        ingestor.run()
        return catalog.load_table(table_id).scan().to_arrow()

    return run
