from dataclasses import dataclass, field
from typing import Callable
from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from streaming.init import initialize_spark
from streaming.pipelines.base import BatchPipeline
from streaming.assets.cleaning import trim_whitespace, replace_substring, parse_date, empty_to_null
from streaming.schemas import ELECTRICITY_TARIFF_SCHEMA


def fix_decimal_separator(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="price_per_kwh_mkd", current=",", replacement=".")

def parse_valid_from(df: DataFrame) -> DataFrame:
    return parse_date(df, column="valid_from", date_format="dd.MM.yyyy")


@dataclass
class ElectricityTariffsPipeline(BatchPipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        fix_decimal_separator,
        parse_valid_from,
    ])

if __name__ == '__main__':
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    electricity_tariffs_pipeline: ElectricityTariffsPipeline = ElectricityTariffsPipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_SCHEMA,
        config=config_loader.get_erc()
    )
    electricity_tariffs_pipeline.run()
