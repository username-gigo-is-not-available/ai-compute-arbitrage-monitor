from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame, DataFrameWriterV2, SparkSession
from pyspark.sql.functions import col, hours as spark_hours, max as spark_max
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException


class SilverWriteStrategy(ABC):
    def __init__(self, column: str) -> None:
        self.column = column

    @abstractmethod
    def read_filter(self, df: DataFrame, session: SparkSession, table_name: str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def write(self, df: DataFrame, table_name: str) -> None:
        raise NotImplementedError

    @staticmethod
    def _write_or_create(df: DataFrame, table_name: str,
                         write: Callable[[DataFrameWriterV2], None], partition: Column | str) -> None:
        try:
            SilverWriteStrategy._add_missing_columns(df, table_name)
            write(df.writeTo(table_name))
        except AnalysisException:
            df.writeTo(table_name).using("iceberg").partitionedBy(partition).create()

    @staticmethod
    def _add_missing_columns(df: DataFrame, table_name: str) -> None:
        # A Silver schema that gained columns (e.g. ADR-020's slice columns) adds them to the existing table at the
        # batch's position; existing columns are left as they are. A missing table raises, so the caller creates it.
        existing = set(df.sparkSession.table(table_name).columns)
        previous = None
        for f in df.schema.fields:
            if f.name not in existing:
                position = f"AFTER `{previous}`" if previous else "FIRST"
                df.sparkSession.sql(
                    f"ALTER TABLE {table_name} ADD COLUMN `{f.name}` {f.dataType.simpleString()} {position}"
                )
            previous = f.name


class AppendByHour(SilverWriteStrategy):
    """Event logs: watermark-filtered read on column, append partitioned by hour(column).
    compute_offers uses snapshot_at, so a rerun of an already-refined hour equals the watermark and is ignored (ADR-019)."""

    def __init__(self, column: str = "ingested_at") -> None:
        super().__init__(column)

    def read_filter(self, df: DataFrame, session: SparkSession, table_name: str) -> DataFrame:
        try:
            row = session.table(table_name).agg(spark_max(self.column)).collect()[0]
            watermark = row[0]
            if watermark is not None:
                return df.filter(col(self.column) > watermark)
        except AnalysisException:
            pass
        return df

    def write(self, df: DataFrame, table_name: str) -> None:
        self._write_or_create(df, table_name, lambda writer: writer.append(), spark_hours(self.column))


class OverwriteByPartition(SilverWriteStrategy):
    """Effective-dated data: full Bronze read, overwrite Silver by an identity partition on column
    (valid_from for seeds, the provider's timestamp for exchange_rates). Re-ingesting the same fact replaces it."""

    def __init__(self, column: str = "valid_from") -> None:
        super().__init__(column)

    def read_filter(self, df: DataFrame, session: SparkSession, table_name: str) -> DataFrame:
        return df

    def write(self, df: DataFrame, table_name: str) -> None:
        self._write_or_create(df, table_name, lambda writer: writer.overwritePartitions(), self.column)


@dataclass(frozen=True)
class SilverTable:
    """A Silver table's schema and the strategy that reads Bronze into it and writes it."""
    schema: StructType
    write_strategy: SilverWriteStrategy

    def read_filter(self, df: DataFrame, session: SparkSession, table_name: str) -> DataFrame:
        return self.write_strategy.read_filter(df, session, table_name)

    def write(self, df: DataFrame, table_name: str) -> None:
        self.write_strategy.write(df, table_name)
