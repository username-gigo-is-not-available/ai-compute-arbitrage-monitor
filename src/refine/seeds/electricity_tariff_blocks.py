from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from config.apis.evn import EVNConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from refine.assets.cleaning import trim_whitespace, empty_to_null
from refine.assets.extraction import extract_pattern, extract_valid_from_date
from refine.assets.patterns import KWH_BOUNDS_PATTERN, BLOCK_NUMBER_PATTERN
from refine.init import initialize_spark
from refine.base import Pipeline
from refine.schemas.electricity_tariff_blocks import ELECTRICITY_TARIFF_BLOCKS_SCHEMA


def extract_block_number(df: DataFrame) -> DataFrame:
    return extract_pattern(df,
                           source_column="block_number_text",
                           target_column="block_number",
                           pattern=BLOCK_NUMBER_PATTERN)


def extract_lower_bound_kwh(df: DataFrame) -> DataFrame:
    return extract_pattern(df,
                           source_column="kwh_boundaries_text",
                           target_column="lower_bound_kwh",
                           pattern=KWH_BOUNDS_PATTERN,
                           group=1)

def extract_upper_bound_kwh(df: DataFrame) -> DataFrame:
    return extract_pattern(df,
                           source_column="kwh_boundaries_text",
                           target_column="upper_bound_kwh",
                           pattern=KWH_BOUNDS_PATTERN,
                           group=2,
                           nullable=True)


@dataclass
class ElectricityTariffBlocksPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        extract_block_number,
        extract_lower_bound_kwh,
        extract_upper_bound_kwh,
        extract_valid_from_date
    ])


def run():
    session: SparkSession = initialize_spark()
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_blocks: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_BLOCKS,
                                                 dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_blocks_pipeline: ElectricityTariffBlocksPipeline = ElectricityTariffBlocksPipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_BLOCKS_SCHEMA,
        dataset=electricity_tariff_blocks,
        config=evn_config,
        storage_config=storage_config,
    )
    electricity_tariff_blocks_pipeline.run()


if __name__ == '__main__':
    run()
