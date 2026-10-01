from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, hours as spark_hours, max as spark_max
from pyspark.sql.utils import AnalysisException


class SilverWriteStrategy(ABC):
    @abstractmethod
    def read_filter(self, df: DataFrame, session: SparkSession, silver_table: str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def write(self, df: DataFrame, silver_table: str) -> None:
        raise NotImplementedError


class IncrementalAppend(SilverWriteStrategy):
    """Sources: watermark-filtered read, append partitioned by hour(ingested_at)."""

    def read_filter(self, df: DataFrame, session: SparkSession, silver_table: str) -> DataFrame:
        try:
            row = session.table(silver_table).agg(spark_max("ingested_at")).collect()[0]
            watermark = row[0]
            if watermark is not None:
                return df.filter(col("ingested_at") > watermark)
        except AnalysisException:
            pass
        return df

    def write(self, df: DataFrame, silver_table: str) -> None:
        try:
            df.writeTo(silver_table).append()
        except AnalysisException:
            df.writeTo(silver_table).using("iceberg").partitionedBy(spark_hours("ingested_at")).create()


class PartitionedOverwrite(SilverWriteStrategy):
    """Full Bronze read, overwrite Silver by an effective-date partition (valid_from for seeds,
    the provider's timestamp for exchange_rates). Re-ingesting the same fact replaces it."""

    def __init__(self, partition_column: str = "valid_from") -> None:
        self.partition_column = partition_column

    def read_filter(self, df: DataFrame, session: SparkSession, silver_table: str) -> DataFrame:
        return df

    def write(self, df: DataFrame, silver_table: str) -> None:
        try:
            df.writeTo(silver_table).overwritePartitions()
        except AnalysisException:
            df.writeTo(silver_table).using("iceberg").partitionedBy(self.partition_column).create()
