from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from config.apis.evn import EVNConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from refine.assets.cleaning import trim_whitespace, replace_substring, empty_to_null
from refine.assets.extraction import extract_valid_from_date
from refine.init import initialize_spark
from refine.base import Pipeline
from refine.schemas.electricity_tariff_fees import ELECTRICITY_TARIFF_FEES_SCHEMA


def replace_decimal_separator(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="value", current=",", replacement=".")


def standardize_currency_symbol(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="metric", current="денари", replacement="MKD")


def standardize_label(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="label", current="Активна ел. енергија",
                             replacement="Активна електрична енергија")


@dataclass
class ElectricityTariffFeesPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        replace_decimal_separator,
        standardize_currency_symbol,
        extract_valid_from_date,
        standardize_label
    ])


def run():
    session: SparkSession = initialize_spark()
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_fees: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_FEES,
                                               dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_prices_pipeline: ElectricityTariffFeesPipeline = ElectricityTariffFeesPipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_FEES_SCHEMA,
        dataset=electricity_tariff_fees,
        config=evn_config,
        storage_config=storage_config,
    )
    electricity_tariff_prices_pipeline.run()


if __name__ == '__main__':
    run()
