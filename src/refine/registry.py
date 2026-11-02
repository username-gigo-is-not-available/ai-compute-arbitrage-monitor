from dataclasses import dataclass

from pyiceberg.transforms import DayTransform

from common.enums import DatasetName
from config.datasets import DATASETS
from config.loader import ConfigLoader
from refine.base import Pipeline
from refine.init import initialize_spark
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.compute_offers import COMPUTE_OFFERS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_blocks import ELECTRICITY_TARIFF_BLOCKS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_fees import ELECTRICITY_TARIFF_FEES_SILVER_SCHEMA
from refine.schemas.electricity_tariff_tiers import ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA
from refine.schemas.exchange_rates import EXCHANGE_RATES_SILVER_SCHEMA
from refine.seeds.electricity_tariff_blocks import ElectricityTariffBlocksPipeline
from refine.seeds.electricity_tariff_fees import ElectricityTariffFeesPipeline
from refine.seeds.electricity_tariff_tiers import ElectricityTariffTiersPipeline
from refine.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowSchedulePipeline
from refine.sources.compute_offers import ComputeOffersPipeline
from refine.sources.exchange_rates import ExchangeRatesPipeline


# The Silver side of each dataset: which pipeline cleans it and how its table is laid out. Type and settings come
# from config.datasets.DATASETS.
@dataclass(frozen=True)
class RefineSpec:
    pipeline: type[Pipeline]
    silver_table: SilverTable


REFINE_SPECS: dict[DatasetName, RefineSpec] = {
    DatasetName.COMPUTE_OFFERS: RefineSpec(
        pipeline=ComputeOffersPipeline,
        silver_table=SilverTable(COMPUTE_OFFERS_SILVER_SCHEMA,
                                 OverwriteByPartition(column="snapshot_at", transform=DayTransform())),
    ),
    DatasetName.EXCHANGE_RATES: RefineSpec(
        pipeline=ExchangeRatesPipeline,
        silver_table=SilverTable(EXCHANGE_RATES_SILVER_SCHEMA, OverwriteByPartition(column="timestamp")),
    ),
    DatasetName.ELECTRICITY_TARIFF_TIERS: RefineSpec(
        pipeline=ElectricityTariffTiersPipeline,
        silver_table=SilverTable(ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA, OverwriteByPartition()),
    ),
    DatasetName.ELECTRICITY_TARIFF_FEES: RefineSpec(
        pipeline=ElectricityTariffFeesPipeline,
        silver_table=SilverTable(ELECTRICITY_TARIFF_FEES_SILVER_SCHEMA, OverwriteByPartition()),
    ),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS: RefineSpec(
        pipeline=ElectricityTariffBlocksPipeline,
        silver_table=SilverTable(ELECTRICITY_TARIFF_BLOCKS_SILVER_SCHEMA, OverwriteByPartition()),
    ),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE: RefineSpec(
        pipeline=ElectricityTariffWindowSchedulePipeline,
        silver_table=SilverTable(ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA, OverwriteByPartition()),
    ),
}


def run_refine(dataset: str) -> None:
    name = DatasetName(dataset)
    declaration = DATASETS[name]
    spec = REFINE_SPECS[name]
    loader = ConfigLoader()
    config = declaration.config(loader)
    if not config.enabled:
        return
    spec.pipeline(
        session=initialize_spark(),
        dataset=declaration.dataset,
        config=config,
        lakehouse_config=loader.get_lakehouse(),
        silver_table=spec.silver_table,
    ).run()
