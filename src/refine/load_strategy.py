from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import StructField, StructType

from refine.schemas.base import SILVER_META_COLUMNS_SCHEMA


class LoadStrategy(ABC):
    def __init__(self, column: str) -> None:
        self.column = column

    @abstractmethod
    def read(self, session: SparkSession, bronze_table: str, silver_table: str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def partition_spec(self, schema: Schema) -> PartitionSpec:
        raise NotImplementedError

    @abstractmethod
    def write(self, df: DataFrame, table_name: str) -> None:
        raise NotImplementedError


class AppendByDay(LoadStrategy):

    def __init__(self, column: str) -> None:
        super().__init__(column)

    def read(self, session: SparkSession, bronze_table: str, silver_table: str) -> DataFrame:
        bronze = session.table(bronze_table)
        watermark = session.table(silver_table).agg(spark_max(self.column)).collect()[0][0]
        return bronze if watermark is None else bronze.filter(col(self.column) > watermark)

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=DayTransform(), name=f"{self.column}_day") # noqa
        )

    def write(self, df: DataFrame, table_name: str) -> None:
        df.writeTo(table_name).append()


class OverwriteByPartition(LoadStrategy):

    def __init__(self, column: str = "valid_from") -> None:
        super().__init__(column)

    def read(self, session: SparkSession, bronze_table: str, silver_table: str) -> DataFrame:
        return session.table(bronze_table)

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=IdentityTransform(), name=self.column) # noqa
        )

    def write(self, df: DataFrame, table_name: str) -> None:
        df.writeTo(table_name).overwritePartitions()


@dataclass(frozen=True)
class SilverTable:
    schema: StructType
    load_strategy: LoadStrategy

    def table_schema(self) -> StructType:
        return StructType(self.schema.fields + SILVER_META_COLUMNS_SCHEMA.fields)

    def iceberg_schema(self) -> Schema:
        optional = StructType([StructField(f.name, f.dataType, nullable=True) for f in self.table_schema().fields])
        return assign_fresh_schema_ids(Catalog._convert_schema_if_needed(to_arrow_schema(optional)))

    def partition_spec(self) -> PartitionSpec:
        return self.load_strategy.partition_spec(self.iceberg_schema())

    def read(self, session: SparkSession, bronze_table: str, silver_table: str) -> DataFrame:
        return self.load_strategy.read(session, bronze_table, silver_table)

    def write(self, df: DataFrame, table_name: str) -> None:
        self.load_strategy.write(df, table_name)
