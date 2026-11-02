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
    name: DatasetName
    dataset_type: DatasetType
    config: Callable[[ConfigLoader], DatasetConfig]

    @property
    def dataset(self) -> Dataset:
        return Dataset(dataset_name=self.name, dataset_type=self.dataset_type)


DATASETS: dict[DatasetName, DatasetDeclaration] = {
    DatasetName.COMPUTE_OFFERS: DatasetDeclaration(
        DatasetName.COMPUTE_OFFERS, DatasetType.SOURCES, ConfigLoader.get_vast_ai),
    DatasetName.EXCHANGE_RATES: DatasetDeclaration(
        DatasetName.EXCHANGE_RATES, DatasetType.SOURCES, ConfigLoader.get_exchange_rate),
    DatasetName.ELECTRICITY_TARIFF_TIERS: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_TIERS, DatasetType.SEEDS, ConfigLoader.get_evn),
    DatasetName.ELECTRICITY_TARIFF_FEES: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_FEES, DatasetType.SEEDS, ConfigLoader.get_evn),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_BLOCKS, DatasetType.SEEDS, ConfigLoader.get_evn),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE, DatasetType.SEEDS, ConfigLoader.get_evn),
}
