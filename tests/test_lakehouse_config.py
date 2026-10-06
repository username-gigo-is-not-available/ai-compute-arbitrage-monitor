"""Unit tests for GCPLakehouseConfig.

Run directly:   uv run python tests/test_lakehouse_config.py
Under pytest:   uv run --extra dev pytest tests/test_lakehouse_config.py
"""

from __future__ import annotations

import sys
import unittest

from common.classes import Dataset
from common.enums import DatasetName, DatasetType, DataStageType
from config.lakehouse import GCPLakehouseConfig
from ingest.schemas.compute_offers import COMPUTE_OFFERS_BRONZE_SCHEMA
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import update_table_metadata


def _config() -> GCPLakehouseConfig:
    return GCPLakehouseConfig(
        catalog_id="ai-compute-arbitrage-monitor-catalog",
        project_id="graphic-mission-505412-j7",
        warehouse="bl://projects/graphic-mission-505412-j7/catalogs/ai-compute-arbitrage-monitor-catalog",
    )


class TestNamespace(unittest.TestCase):

    def test_sources(self):
        cfg = _config()
        ds = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
        self.assertEqual(cfg.namespace(DataStageType.BRONZE, ds), "bronze_sources")
        self.assertEqual(cfg.namespace(DataStageType.SILVER, ds), "silver_sources")

    def test_seeds(self):
        cfg = _config()
        ds = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS)
        self.assertEqual(cfg.namespace(DataStageType.BRONZE, ds), "bronze_seeds")
        self.assertEqual(cfg.namespace(DataStageType.SILVER, ds), "silver_seeds")


class TestTableId(unittest.TestCase):

    def test_tuple_form(self):
        cfg = _config()
        ds = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
        ns, name = cfg.table_id(DataStageType.BRONZE, ds)
        self.assertEqual(ns, "bronze_sources")
        self.assertEqual(name, "compute_offers")


class TestSparkTable(unittest.TestCase):

    def test_format(self):
        cfg = _config()
        ds = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
        self.assertEqual(
            cfg.spark_table(DataStageType.BRONZE, ds),
            "ai-compute-arbitrage-monitor-catalog.bronze_sources.compute_offers",
        )

    def test_seed(self):
        cfg = _config()
        ds = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS)
        self.assertEqual(
            cfg.spark_table(DataStageType.SILVER, ds),
            "ai-compute-arbitrage-monitor-catalog.silver_seeds.electricity_tariff_tiers",
        )


class _FakeCatalog:
    def __init__(self, namespaces: set[str] | None = None, tables: set[tuple[str, str]] | None = None):
        self.namespaces = namespaces or set()
        self.tables = tables or set()
        self.created_tables: list[tuple[str, str]] = []

    def create_namespace(self, namespace: str) -> None:
        if namespace in self.namespaces:
            raise NamespaceAlreadyExistsError(namespace)
        self.namespaces.add(namespace)

    def table_exists(self, identifier: tuple[str, str]) -> bool:
        return identifier in self.tables

    def create_table(self, identifier: tuple[str, str], schema, partition_spec) -> None:
        self.tables.add(identifier)
        self.created_tables.append(identifier)


class TestEnsure(unittest.TestCase):
    DS = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)

    def test_stage_namespace_matches_namespace(self):
        self.assertEqual(GCPLakehouseConfig.stage_namespace(DataStageType.BRONZE, DatasetType.SEEDS), "bronze_seeds")
        self.assertEqual(GCPLakehouseConfig.stage_namespace(DataStageType.BRONZE, DatasetType.SOURCES),
                         _config().namespace(DataStageType.BRONZE, self.DS))

    def test_ensure_namespace_tolerates_existing(self):
        catalog = _FakeCatalog(namespaces={"silver_sources"})
        _config().ensure_namespace(catalog, DataStageType.SILVER, self.DS)
        self.assertEqual(catalog.namespaces, {"silver_sources"})

    def test_ensure_table_creates_namespace_and_missing_table(self):
        catalog = _FakeCatalog()
        _config().ensure_table(catalog, DataStageType.BRONZE, self.DS, schema=None, partition_spec=None)
        self.assertEqual(catalog.namespaces, {"bronze_sources"})
        self.assertEqual(catalog.created_tables, [("bronze_sources", "compute_offers")])

class _MetadataCatalog(_FakeCatalog):
    """Keeps real Iceberg table metadata in memory and applies commits with pyiceberg's own update logic."""

    def __init__(self):
        super().__init__()
        self.metadata = {}

    def create_table(self, identifier, schema, partition_spec) -> None:
        super().create_table(identifier, schema, partition_spec)
        self.metadata[identifier] = new_table_metadata(schema, partition_spec, UNSORTED_SORT_ORDER, location="memory://t")

    def load_table(self, identifier) -> Table:
        return Table(identifier, self.metadata[identifier], "memory://t/metadata.json", PyArrowFileIO(), self)

    def commit_table(self, table, requirements, updates) -> CommitTableResponse:
        self.metadata[table.name()] = update_table_metadata(self.metadata[table.name()], updates)
        return CommitTableResponse(metadata=self.metadata[table.name()], metadata_location="memory://t/metadata.json")


class TestEnsureTableSchemaEvolution(unittest.TestCase):
    """An existing table gains the columns its schema lacks, where the schema puts them."""
    DS = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
    TABLE = ("bronze_sources", "compute_offers")

    def setUp(self):
        self.catalog = _MetadataCatalog()

    def _ensure(self, schema: Schema) -> None:
        _config().ensure_table(self.catalog, DataStageType.BRONZE, self.DS, schema=schema,
                               partition_spec=PartitionSpec())

    def test_existing_table_gains_missing_columns_in_schema_order(self):
        # The Bronze table as created before ADR-020, then ensured with the schema that adds the slice columns.
        new_columns = {"gpu_fraction_of_machine", "gpu_ids"}
        self._ensure(Schema(*[f for f in COMPUTE_OFFERS_BRONZE_SCHEMA.fields if f.name not in new_columns]))
        self._ensure(COMPUTE_OFFERS_BRONZE_SCHEMA)
        names = [f.name for f in self.catalog.load_table(self.TABLE).schema().fields]
        self.assertEqual(names, [f.name for f in COMPUTE_OFFERS_BRONZE_SCHEMA.fields])

    def test_existing_table_with_the_same_schema_is_left_alone(self):
        self._ensure(COMPUTE_OFFERS_BRONZE_SCHEMA)
        schema_id = self.catalog.load_table(self.TABLE).schema().schema_id
        self._ensure(COMPUTE_OFFERS_BRONZE_SCHEMA)
        self.assertEqual(self.catalog.load_table(self.TABLE).schema().schema_id, schema_id)


if __name__ == "__main__":
    sys.exit(unittest.main())
