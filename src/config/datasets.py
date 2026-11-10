from collections.abc import Callable
from dataclasses import dataclass

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from common.types import DatasetConfig
from config.loader import ConfigLoader


# The one list of datasets: each stage's registry adds only its own parts, and the DAG builds one pipeline per entry
@dataclass(frozen=True)
class DatasetDeclaration:
    name: DatasetName
    dataset_type: DatasetType
    config: Callable[[ConfigLoader], DatasetConfig]
    description: str

    @property
    def dataset(self) -> Dataset:
        return Dataset(dataset_name=self.name, dataset_type=self.dataset_type)


DATASETS: dict[DatasetName, DatasetDeclaration] = {
    DatasetName.COMPUTE_OFFERS: DatasetDeclaration(
        DatasetName.COMPUTE_OFFERS, DatasetType.SOURCES, ConfigLoader.get_vast_ai, "Vast.ai compute offers"
    ),
    DatasetName.EXCHANGE_RATES: DatasetDeclaration(
        DatasetName.EXCHANGE_RATES, DatasetType.SOURCES, ConfigLoader.get_exchange_rate, "MKD/USD exchange rate"
    ),
    DatasetName.ELECTRICITY_TARIFF_TIERS: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_TIERS, DatasetType.SEEDS, ConfigLoader.get_evn, "EVN tariff tiers"
    ),
    DatasetName.ELECTRICITY_TARIFF_FEES: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_FEES, DatasetType.SEEDS, ConfigLoader.get_evn, "EVN tariff fees"
    ),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_BLOCKS, DatasetType.SEEDS, ConfigLoader.get_evn, "EVN tariff blocks"
    ),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE: DatasetDeclaration(
        DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
        DatasetType.SEEDS,
        ConfigLoader.get_evn,
        "EVN tariff window schedule",
    ),
}
