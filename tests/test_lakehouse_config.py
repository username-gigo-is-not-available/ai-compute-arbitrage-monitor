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
from pyiceberg.exceptions import NamespaceAlreadyExistsError


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

    def test_ensure_table_leaves_existing_table(self):
        catalog = _FakeCatalog(namespaces={"bronze_sources"}, tables={("bronze_sources", "compute_offers")})
        _config().ensure_table(catalog, DataStageType.BRONZE, self.DS, schema=None, partition_spec=None)
        self.assertEqual(catalog.created_tables, [])


if __name__ == "__main__":
    sys.exit(unittest.main())
