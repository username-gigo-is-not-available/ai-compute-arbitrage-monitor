from pyspark.sql import SparkSession

from config.loader import ConfigLoader
from config.spark import configure_spark


def initialize_spark() -> SparkSession:
    loader = ConfigLoader()
    builder = configure_spark(
        SparkSession.builder.appName("ai-compute-arbitrage-monitor"),
        loader.get_lakehouse(),
        loader.get_execution_type(),
    )
    return builder.getOrCreate()
