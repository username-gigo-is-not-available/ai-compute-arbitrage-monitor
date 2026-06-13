from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from common.classes import Dataset
from common.enums import DatasetType, DatasetName
from config.loader import ConfigLoader
from config.apis.evn import EVNConfig
from config.storage import GCPStorageConfig
from refine.assets.filtering import deduplicate
from refine.init import initialize_spark
from refine.base import Pipeline
from refine.assets.cleaning import trim_whitespace, empty_to_null, parse_valid_from
from refine.schemas.electricity_tariff_schedule import ELECTRICITY_TARIFF_SCHEDULE_SCHEMA


def deduplicate_electricity_tariffs_schedule(df: DataFrame) -> DataFrame:
    return deduplicate(df, columns=['day_of_week', 'start_hour', 'end_hour', 'tariff_type', 'valid_from'])


@dataclass
class ElectricityTariffSchedulePipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        parse_valid_from,
        deduplicate_electricity_tariffs_schedule
    ])


def run():
    session: SparkSession = initialize_spark()
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_schedule: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_SCHEDULE, dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariffs_schedule_pipeline: ElectricityTariffSchedulePipeline = ElectricityTariffSchedulePipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_SCHEDULE_SCHEMA,
        dataset=electricity_tariff_schedule,
        config=evn_config,
        storage_config=storage_config,
    )

    electricity_tariffs_schedule_pipeline.run()


if __name__ == "__main__":
    run()
