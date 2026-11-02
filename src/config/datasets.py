from dataclasses import dataclass
from typing import Callable

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from common.types import DatasetConfig
from config.loader import ConfigLoader


# What every stage needs to know about a dataset: its type (which namespace it lives in) and which settings block
# configures it. Each stage's registry (ingest.registry, refine.registry) adds only its own parts.
@dataclass(frozen=True)
class DatasetDeclaration:
    dataset: Dataset
    config: Callable[[ConfigLoader], DatasetConfig]


def _declare(name: DatasetName, dataset_type: DatasetType,
             config: Callable[[ConfigLoader], DatasetConfig]) -> DatasetDeclaration:
    return DatasetDeclaration(dataset=Dataset(dataset_name=name, dataset_type=dataset_type), config=config)


DATASETS: dict[DatasetName, DatasetDeclaration] = {
    DatasetName.COMPUTE_OFFERS: _declare(DatasetName.COMPUTE_OFFERS, DatasetType.SOURCES, ConfigLoader.get_vast_ai),
    DatasetName.EXCHANGE_RATES: _declare(DatasetName.EXCHANGE_RATES, DatasetType.SOURCES,
                                         ConfigLoader.get_exchange_rate),
    DatasetName.ELECTRICITY_TARIFF_TIERS: _declare(DatasetName.ELECTRICITY_TARIFF_TIERS, DatasetType.SEEDS,
                                                   ConfigLoader.get_evn),
    DatasetName.ELECTRICITY_TARIFF_FEES: _declare(DatasetName.ELECTRICITY_TARIFF_FEES, DatasetType.SEEDS,
                                                  ConfigLoader.get_evn),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS: _declare(DatasetName.ELECTRICITY_TARIFF_BLOCKS, DatasetType.SEEDS,
                                                    ConfigLoader.get_evn),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE: _declare(DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
                                                             DatasetType.SEEDS, ConfigLoader.get_evn),
}
