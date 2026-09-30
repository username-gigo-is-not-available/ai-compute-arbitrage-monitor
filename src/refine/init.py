from pyspark.sql import SparkSession

from config.loader import ConfigLoader


def initialize_spark() -> SparkSession:
    loader = ConfigLoader()
    builder = loader.get_lakehouse().configure_spark(
        SparkSession.builder.appName("ai-compute-arbitrage-monitor"),
        loader.get_execution_type(),
    )
    return builder.getOrCreate()
