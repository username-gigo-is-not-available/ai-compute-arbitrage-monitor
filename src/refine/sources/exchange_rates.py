from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame

from refine.assets.filtering import deduplicate_keep_latest
from refine.assets.cleaning import trim_whitespace, empty_to_null
from refine.base import Pipeline


def deduplicate_exchange_rate(df: DataFrame) -> DataFrame:
    return deduplicate_keep_latest(df, columns=['from_currency', 'to_currency', 'timestamp'], order_by='ingested_at')


@dataclass
class ExchangeRatesPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        deduplicate_exchange_rate
    ])
