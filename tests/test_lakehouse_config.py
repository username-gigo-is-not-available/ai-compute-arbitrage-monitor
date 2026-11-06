"""GCPLakehouseConfig names namespaces and tables."""

from common.classes import Dataset
from common.enums import DatasetName, DatasetType, DataStageType
from config.lakehouse import GCPLakehouseConfig

CONFIG = GCPLakehouseConfig(
    catalog_id="ai-compute-arbitrage-monitor-catalog",
    project_id="graphic-mission-505412-j7",
    warehouse="bl://projects/graphic-mission-505412-j7/catalogs/ai-compute-arbitrage-monitor-catalog",
)
COMPUTE_OFFERS = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
TARIFF_TIERS = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS)


class TestNamespace:
    def test_sources(self):
        assert CONFIG.namespace(DataStageType.BRONZE, COMPUTE_OFFERS) == "bronze_sources"
        assert CONFIG.namespace(DataStageType.SILVER, COMPUTE_OFFERS) == "silver_sources"

    def test_seeds(self):
        assert CONFIG.namespace(DataStageType.BRONZE, TARIFF_TIERS) == "bronze_seeds"
        assert CONFIG.namespace(DataStageType.SILVER, TARIFF_TIERS) == "silver_seeds"


def test_table_id_is_namespace_and_name():
    assert CONFIG.table_id(DataStageType.BRONZE, COMPUTE_OFFERS) == ("bronze_sources", "compute_offers")


class TestSparkTable:
    def test_format(self):
        assert (
            CONFIG.spark_table(DataStageType.BRONZE, COMPUTE_OFFERS)
            == "ai-compute-arbitrage-monitor-catalog.bronze_sources.compute_offers"
        )

    def test_seed(self):
        assert (
            CONFIG.spark_table(DataStageType.SILVER, TARIFF_TIERS)
            == "ai-compute-arbitrage-monitor-catalog.silver_seeds.electricity_tariff_tiers"
        )


def test_stage_namespace_matches_namespace():
    assert GCPLakehouseConfig.stage_namespace(DataStageType.BRONZE, DatasetType.SEEDS) == "bronze_seeds"
    assert GCPLakehouseConfig.stage_namespace(DataStageType.BRONZE, DatasetType.SOURCES) == CONFIG.namespace(
        DataStageType.BRONZE, COMPUTE_OFFERS
    )
