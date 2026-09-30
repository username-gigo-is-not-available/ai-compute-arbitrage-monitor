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
            "ai_compute_arbitrage_monitor_catalog.bronze_sources.compute_offers",
        )

    def test_seed(self):
        cfg = _config()
        ds = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS)
        self.assertEqual(
            cfg.spark_table(DataStageType.SILVER, ds),
            "ai_compute_arbitrage_monitor_catalog.silver_seeds.electricity_tariff_tiers",
        )


if __name__ == "__main__":
    sys.exit(unittest.main())
