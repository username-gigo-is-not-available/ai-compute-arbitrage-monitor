import logging
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from common.classes import Dataset
from common.enums import DataStageType
from common.types import DatasetConfig
from config.storage import GCPStorageConfig
from refine.assets.casting import cast_to_schema
from refine.assets.extraction import add_processed_at_column


@dataclass
class Pipeline:
    session: SparkSession
    schema: StructType
    dataset: Dataset
    config: DatasetConfig
    storage_config: GCPStorageConfig
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=list)
    logger: logging.Logger = field(init=False)

    def __post_init__(self):
        self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    def read(self) -> DataFrame:
        input_path: str = self.storage_config.directory_path_with_extension(stage=DataStageType.BRONZE, dataset=self.dataset)
        self.logger.info(f"Reading from {input_path}")
        return self.session.read.parquet(input_path)

    def save(self, df: DataFrame) -> DataFrame:
        output_path: str = self.storage_config.directory_path_with_extension(stage=DataStageType.SILVER, dataset=self.dataset)
        self.logger.info(f"Writing to {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info("Write complete")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        for step in self.transform_steps:
            step_name = getattr(step, "__name__", repr(step))
            self.logger.info(f"Applying transform step: {step_name}")
            df = df.transform(step)
        self.logger.info("Applying cast_to_schema")
        df = df.transform(add_processed_at_column)
        return cast_to_schema(df, self.schema)

    def generate(self, df: DataFrame) -> DataFrame | None:
        return None

    def run(self):
        name = self.__class__.__name__
        self.logger.info(f"{name} starting")
        df = self.read()
        self.logger.info(f"{name} read complete")
        generated_df = self.generate(df)
        if generated_df is not None:
            self.logger.info(f"{name} generation complete")
            df = generated_df
        df = self.transform(df)
        self.logger.info(f"{name} transform complete")
        result = self.save(df)
        self.logger.info(f"{name} complete")
        return result
