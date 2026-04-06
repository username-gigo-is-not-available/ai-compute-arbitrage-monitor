from pyspark.sql import SparkSession


def initialize_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ai_arbitrage_monitor")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeout", "300s")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

