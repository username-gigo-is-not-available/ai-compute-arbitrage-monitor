import os
import sys

from pyspark.sql import SparkSession

from config.loader import ConfigLoader
from config.spark import configure_spark


def initialize_spark() -> SparkSession:
    # Spark worker processes inherit PYSPARK_PYTHON; must be set before SparkContext starts.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    loader = ConfigLoader()
    builder = configure_spark(
        SparkSession.builder.appName("ai-compute-arbitrage-monitor"),
        loader.get_lakehouse(),
        loader.get_execution_type(),
    )
    return builder.getOrCreate()
