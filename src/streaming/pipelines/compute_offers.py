from dataclasses import dataclass, field
from typing import Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from config.kafka import KafkaConfig
from config.loader import SilverConfigLoader
from streaming.init import initialize_spark
from streaming.pipelines.base import StreamPipeline
from streaming.assets.cleaning import strip_non_ascii, trim_whitespace, replace_substring, empty_to_null
from streaming.schemas import COMPUTE_OFFER_SCHEMA


def strip_cpu_core_suffix(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="cpu_model_name", current=r"\s+\d+-Core(s)? Processor$", replacement="")


@dataclass
class ComputeOffersPipeline(StreamPipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        strip_non_ascii,
        trim_whitespace,
        empty_to_null,
        strip_cpu_core_suffix,
    ])

if __name__ == '__main__':
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    kafka_config: KafkaConfig = config_loader.get_kafka()
    compute_offers_pipeline: ComputeOffersPipeline = ComputeOffersPipeline(
        session=session,
        schema=COMPUTE_OFFER_SCHEMA,
        config=config_loader.get_vast_ai(),
        kafka_config=kafka_config
    )
    query: StreamingQuery = compute_offers_pipeline.run()
    query.awaitTermination()