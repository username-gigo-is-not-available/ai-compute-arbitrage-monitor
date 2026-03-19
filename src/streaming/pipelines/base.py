import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from config.kafka import KafkaConfig
from ingestion.models.types import BatchConfig, StreamConfig
from streaming.assets.casting import cast_to_schema
from streaming.assets.extract import add_processed_at_column


@dataclass
class BasePipeline(ABC):
    session: SparkSession
    schema: StructType
    config: BatchConfig | StreamConfig
    kafka_config: KafkaConfig | None = None
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=list)
    logger: logging.Logger = field(init=False)

    def __post_init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self) -> DataFrame: ...

    def transform(self, df: DataFrame) -> DataFrame:
        for step in self.transform_steps:
            step_name = getattr(step, "__name__", repr(step))
            self.logger.info(f"Applying transform step: {step_name}")
            df = df.transform(step)
        self.logger.info("Applying cast_to_schema")
        df = df.transform(add_processed_at_column)
        return cast_to_schema(df, self.schema)

    @abstractmethod
    def save(self, df: DataFrame) -> DataFrame | StreamingQuery: ...

    def run(self):
        name = self.__class__.__name__
        self.logger.info(f"{name} starting")
        df = self.read()
        self.logger.info(f"{name} read complete")
        df = self.transform(df)
        self.logger.info(f"{name} transform complete")
        result = self.save(df)
        self.logger.info(f"{name} complete")
        return result

@dataclass
class BatchPipeline(BasePipeline, ABC):

    def read(self) -> DataFrame:
        input_path: Path = self.config.input_path_with_suffix(".parquet")
        self.logger.info(f"Reading from {input_path}")
        return self.session.read.parquet(str(input_path))

    def save(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"Writing to {self.config.output_path}")
        df.write.mode("overwrite").parquet(str(self.config.output_path))
        self.logger.info("Write complete")
        return df


@dataclass
class StreamPipeline(BasePipeline, ABC):

    def read(self) -> DataFrame:
        self.logger.info(
            f"Subscribing to topic '{self.config.topic_name}' on {self.kafka_config.bootstrap_servers}")
        return (
            self.session.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_config.bootstrap_servers)
            .option("subscribe", self.config.topic_name)
            .option("startingOffsets", "earliest")
            .load()
            .select(
                F.from_json(F.col("value").cast("string"), self.schema).alias("data"),
            )
            .select("data.*")
        )

    def save(self, df: DataFrame) -> StreamingQuery:
        self.logger.info(f"Starting stream write to {self.config.output_path}")
        return (
            df.writeStream
            .format("parquet")
            .option("path", str(self.config.output_path))
            .option("checkpointLocation", str(self.config.checkpoint_path))
            .outputMode("append")
            .start()
        )
