from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from refine.assets.cleaning import trim_whitespace, replace_substring, empty_to_null, parse_valid_from
from refine.assets.filtering import deduplicate
from refine.init import initialize_spark
from refine.pipelines.base import Pipeline
from refine.schemas.electricity_tariff import ELECTRICITY_TARIFF_SCHEMA


def fix_decimal_separator(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="price_mkd_per_kwh", current=",", replacement=".")


def deduplicate_electricity_tariffs_prices(df: DataFrame) -> DataFrame:
    return deduplicate(df, columns=['tariff_description', 'valid_from'])


@dataclass
class ElectricityTariffPricesPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        fix_decimal_separator,
        parse_valid_from,
        deduplicate_electricity_tariffs_prices
    ])


def run():
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    electricity_tariff_prices_pipeline: ElectricityTariffPricesPipeline = ElectricityTariffPricesPipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_SCHEMA,
        config=config_loader.get_erc()
    )
    electricity_tariff_prices_pipeline.run()


if __name__ == '__main__':
    run()
