from dataclasses import dataclass

from common.enums import DatasetName
from config.datasets import DATASETS
from config.loader import ConfigLoader
from refine.base import Pipeline
from refine.init import initialize_spark
from refine.load_strategy import SilverTable
from refine.seeds.electricity_tariff_blocks import ElectricityTariffBlocksPipeline
from refine.seeds.electricity_tariff_fees import ElectricityTariffFeesPipeline
from refine.seeds.electricity_tariff_tiers import ElectricityTariffTiersPipeline
from refine.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowSchedulePipeline
from refine.sources.compute_offers import ComputeOffersPipeline
from refine.sources.exchange_rates import ExchangeRatesPipeline
from refine.tables import SILVER_TABLES


# The Silver side of each dataset that loads data (DML): which pipeline cleans it and which table it writes.
# The table's shape is in refine.tables; type and settings come from config.datasets.DATASETS.
@dataclass(frozen=True)
class RefineSpec:
    pipeline: type[Pipeline]
    silver_table: SilverTable


REFINE_SPECS: dict[DatasetName, RefineSpec] = {
    DatasetName.COMPUTE_OFFERS: RefineSpec(
        pipeline=ComputeOffersPipeline,
        silver_table=SILVER_TABLES[DatasetName.COMPUTE_OFFERS],
    ),
    DatasetName.EXCHANGE_RATES: RefineSpec(
        pipeline=ExchangeRatesPipeline,
        silver_table=SILVER_TABLES[DatasetName.EXCHANGE_RATES],
    ),
    DatasetName.ELECTRICITY_TARIFF_TIERS: RefineSpec(
        pipeline=ElectricityTariffTiersPipeline,
        silver_table=SILVER_TABLES[DatasetName.ELECTRICITY_TARIFF_TIERS],
    ),
    DatasetName.ELECTRICITY_TARIFF_FEES: RefineSpec(
        pipeline=ElectricityTariffFeesPipeline,
        silver_table=SILVER_TABLES[DatasetName.ELECTRICITY_TARIFF_FEES],
    ),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS: RefineSpec(
        pipeline=ElectricityTariffBlocksPipeline,
        silver_table=SILVER_TABLES[DatasetName.ELECTRICITY_TARIFF_BLOCKS],
    ),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE: RefineSpec(
        pipeline=ElectricityTariffWindowSchedulePipeline,
        silver_table=SILVER_TABLES[DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE],
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
