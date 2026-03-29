from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from config.kafka import KafkaConfig
from config.loader import SilverConfigLoader
from streaming.init import initialize_spark
from streaming.pipelines.base import StreamPipeline
from streaming.assets.cleaning import trim_whitespace, empty_to_null
from streaming.schemas import EXCHANGE_RATE_SCHEMA


@dataclass
class ExchangeRatePipeline(StreamPipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
    ])


if __name__ == '__main__':
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    kafka_config: KafkaConfig = config_loader.get_kafka()
    exchange_rate_pipeline: ExchangeRatePipeline = ExchangeRatePipeline(
        session=session,
        schema=EXCHANGE_RATE_SCHEMA,
        config=config_loader.get_exchange_rate(),
        kafka_config=kafka_config
    )
    query: StreamingQuery = exchange_rate_pipeline.run()

    query.awaitTermination()
