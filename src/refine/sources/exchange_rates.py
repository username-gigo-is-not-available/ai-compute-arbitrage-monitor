from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from config.loader import ConfigLoader
from config.apis.exchange_rate import ExchangeRateConfig
from refine.write_strategy import OverwriteByPartition
from refine.assets.filtering import deduplicate_keep_latest
from refine.init import initialize_spark
from refine.assets.cleaning import trim_whitespace, empty_to_null
from refine.base import Pipeline
from refine.schemas.exchange_rates import EXCHANGE_RATE_SCHEMA


def deduplicate_exchange_rate(df: DataFrame) -> DataFrame:
    return deduplicate_keep_latest(df, columns=['from_currency', 'to_currency', 'timestamp'], order_by='ingested_at')


@dataclass
class ExchangeRatesPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        deduplicate_exchange_rate
    ])


def run():
    session: SparkSession = initialize_spark()
    loader: ConfigLoader = ConfigLoader()
    exchange_rate_config: ExchangeRateConfig = loader.get_exchange_rate()
    exchange_rates: Dataset = Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES)
    if not exchange_rate_config.enabled:
        return

    exchange_rate_pipeline: ExchangeRatesPipeline = ExchangeRatesPipeline(
        session=session,
        schema=EXCHANGE_RATE_SCHEMA,
        dataset=exchange_rates,
        config=exchange_rate_config,
        lakehouse_config=loader.get_lakehouse(),
        write_strategy=OverwriteByPartition(column="timestamp"),
    )
    exchange_rate_pipeline.run()


if __name__ == '__main__':
    run()
