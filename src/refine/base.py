import logging
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from common.classes import Dataset
from common.enums import DataStageType, DatasetType
from common.types import DatasetConfig
from config.lakehouse import GCPLakehouseConfig
from config.storage import GCPStorageConfig
from refine.assets.casting import cast_to_schema
from refine.assets.extraction import add_processed_at_column
from refine.schemas.base import META_COLUMNS_SCHEMA


@dataclass
class Pipeline:
    session: SparkSession
    schema: StructType
    dataset: Dataset
    config: DatasetConfig
    storage_config: GCPStorageConfig
    lakehouse_config: GCPLakehouseConfig
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=list)
    logger: logging.Logger = field(init=False)

    def __post_init__(self):
        self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    def read(self) -> DataFrame:
        bronze = self.lakehouse_config.spark_table(DataStageType.BRONZE, self.dataset)
        self.logger.info(f"Reading from {bronze}")
        df = self.session.table(bronze)

        if self.dataset.dataset_type == DatasetType.SOURCES:
            silver = self.lakehouse_config.spark_table(DataStageType.SILVER, self.dataset)
            watermark = self._watermark(silver)
            if watermark is not None:
                df = df.filter(col("ingested_at") > watermark)
                self.logger.info(f"Watermark filter applied: ingested_at > {watermark}")

        return df

    def _watermark(self, silver_table: str):
        try:
            row = self.session.table(silver_table).agg(spark_max("ingested_at")).collect()[0]
            return row[0]
        except AnalysisException:
            return None

    def save(self, df: DataFrame) -> DataFrame:
        silver = self.lakehouse_config.spark_table(DataStageType.SILVER, self.dataset)
        self.logger.info(f"Writing to {silver}")

        try:
            if self.dataset.dataset_type == DatasetType.SOURCES:
                df.writeTo(silver).append()
            else:
                df.writeTo(silver).overwritePartitions()
        except AnalysisException:
            # Table does not exist yet — create with the correct partition layout
            from pyspark.sql.functions import hours as spark_hours
            if self.dataset.dataset_type == DatasetType.SOURCES:
                df.writeTo(silver).using("iceberg").partitionedBy(spark_hours("ingested_at")).create()
            else:
                df.writeTo(silver).using("iceberg").partitionedBy("valid_from").create()

        self.logger.info("Write complete")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        for step in self.transform_steps:
            step_name = getattr(step, "__name__", repr(step))
            self.logger.info(f"Applying transform step: {step_name}")
            df = df.transform(step)
        self.logger.info("Applying cast_to_schema")
        df = df.transform(add_processed_at_column)
        return cast_to_schema(df, StructType(self.schema.fields + META_COLUMNS_SCHEMA.fields))

    def generate(self, df: DataFrame) -> DataFrame | None:
        return None

    def run(self):
        name = self.__class__.__name__
        self.logger.info(f"{name} starting")
        df = self.read()
        self.logger.info(f"{name} read {df.count()} records")
        generated_df = self.generate(df)
        if generated_df is not None:
            self.logger.info(f"{name} generation complete")
            df = generated_df
        df = self.transform(df)
        self.logger.info(f"{name} transform complete — {df.count()} records")
        result = self.save(df)
        self.logger.info(f"{name} complete")
        return result
