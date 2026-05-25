from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from refine.assets.filtering import deduplicate
from refine.init import initialize_spark
from refine.assets.cleaning import trim_whitespace, empty_to_null
from refine.base import Pipeline
from refine.schemas.exchange_rate import EXCHANGE_RATE_SCHEMA


def deduplicate_exchange_rate(df: DataFrame) -> DataFrame:
    return deduplicate(df, columns=['from_currency', 'to_currency', 'timestamp'])


@dataclass
class ExchangeRatesPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        deduplicate_exchange_rate
    ])


def run():
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    exchange_rate_pipeline: ExchangeRatesPipeline = ExchangeRatesPipeline(
        session=session,
        schema=EXCHANGE_RATE_SCHEMA,
        config=config_loader.get_exchange_rate(),
    )
    exchange_rate_pipeline.run()


if __name__ == '__main__':
    run()
