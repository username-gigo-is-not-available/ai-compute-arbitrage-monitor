import logging
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from common.classes import Dataset
from common.enums import DataStageType
from common.types import DatasetConfig
from config.lakehouse import GCPLakehouseConfig
from refine.assets.casting import cast_to_schema
from refine.assets.extraction import add_processed_at_column
from refine.schemas.base import META_COLUMNS_SCHEMA
from refine.write_strategy import SilverWriteStrategy


@dataclass
class Pipeline:
    session: SparkSession
    schema: StructType
    dataset: Dataset
    config: DatasetConfig
    lakehouse_config: GCPLakehouseConfig
    write_strategy: SilverWriteStrategy
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=list)
    logger: logging.Logger = field(init=False)

    def __post_init__(self):
        self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    def ensure_silver_namespace(self) -> None:
        # Spark creates the Silver table on first write; only the namespace must exist beforehand.
        catalog = self.lakehouse_config.open_catalog()
        self.lakehouse_config.ensure_namespace(catalog, DataStageType.SILVER, self.dataset)

    def read(self) -> DataFrame:
        bronze = self.lakehouse_config.spark_table(DataStageType.BRONZE, self.dataset)
        self.logger.info(f"Reading from {bronze}")
        df = self.session.table(bronze)
        silver = self.lakehouse_config.spark_table(DataStageType.SILVER, self.dataset)
        return self.write_strategy.read_filter(df, self.session, silver)

    def save(self, df: DataFrame) -> DataFrame:
        silver = self.lakehouse_config.spark_table(DataStageType.SILVER, self.dataset)
        self.logger.info(f"Writing to {silver}")
        self.write_strategy.write(df, silver)
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
        self.ensure_silver_namespace()
        df = self.read()
        generated_df = self.generate(df)
        if generated_df is not None:
            self.logger.info(f"{name} generation complete")
            df = generated_df
        df = self.transform(df)
        self.logger.info(f"{name} transform complete — {df.count()} records")
        result = self.save(df)
        self.logger.info(f"{name} complete")
        return result
