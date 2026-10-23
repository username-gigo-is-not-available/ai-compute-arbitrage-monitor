from dataclasses import dataclass

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.transforms import DayTransform, IdentityTransform, Transform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import StructField, StructType

from refine.schemas.base import SILVER_META_COLUMNS_SCHEMA


# Every Silver table reads the Bronze fetches it has not seen yet and overwrites the partitions they touch, so
# Silver is what a rebuild from Bronze would give. Assumes each fetch delivers whole partitions.
class OverwriteByPartition:

    def __init__(self, column: str = "valid_from", transform: Transform = IdentityTransform()) -> None:
        self.column = column
        self.transform = transform

    def read(self, session: SparkSession, bronze_table: str, silver_table: str) -> DataFrame:
        bronze = session.table(bronze_table)
        watermark = session.table(silver_table).agg(spark_max("ingested_at")).collect()[0][0]
        return bronze if watermark is None else bronze.filter(col("ingested_at") > watermark)

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        name = f"{self.column}_day" if isinstance(self.transform, DayTransform) else self.column
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=self.transform, name=name) # noqa
        )

    def write(self, df: DataFrame, table_name: str) -> None:
        df.writeTo(table_name).overwritePartitions()


@dataclass(frozen=True)
class SilverTable:
    schema: StructType
    load_strategy: OverwriteByPartition

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
