from dataclasses import dataclass, field
from typing import Callable
from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from streaming.assets.filtering import deduplicate
from streaming.init import initialize_spark
from streaming.pipelines.base import BatchPipeline
from streaming.assets.cleaning import trim_whitespace, replace_substring, parse_date, empty_to_null, parse_valid_from
from streaming.schemas import ELECTRICITY_TARIFF_SCHEMA


def fix_decimal_separator(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="price_mkd_per_kwh", current=",", replacement=".")

def deduplicate_electricity_tariffs_prices(df: DataFrame) -> DataFrame:
    return deduplicate(df, columns=['tariff_description', 'valid_from'])


@dataclass
class ElectricityTariffPricesPipeline(BatchPipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        fix_decimal_separator,
        parse_valid_from,
        deduplicate_electricity_tariffs_prices
    ])


if __name__ == '__main__':
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    electricity_tariff_prices_pipeline: ElectricityTariffPricesPipeline = ElectricityTariffPricesPipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_SCHEMA,
        config=config_loader.get_erc()
    )
    electricity_tariff_prices_pipeline.run()
