from dataclasses import dataclass, field
from typing import Callable
from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from streaming.init import initialize_spark
from streaming.pipelines.base import BatchPipeline
from streaming.assets.cleaning import strip_non_ascii, trim_whitespace, empty_to_null
from streaming.assets.filtering import filter_integration_test_rows, filter_null
from streaming.schemas import CPU_SPECIFICATION_SCHEMA


def filter_null_tdp(df: DataFrame) -> DataFrame:
    return filter_null(df, "tdp_watts")

@dataclass
class CPUPipeline(BatchPipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        strip_non_ascii,
        trim_whitespace,
        filter_integration_test_rows,
        empty_to_null,
        filter_null_tdp,
    ])


if __name__ == '__main__':
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    cpu_specifications_pipeline: CPUPipeline = CPUPipeline(
        session=session,
        schema=CPU_SPECIFICATION_SCHEMA,
        config=config_loader.get_open_db_cpu()
    )
    cpu_specifications_pipeline.run()
