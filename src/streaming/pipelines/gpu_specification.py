from dataclasses import dataclass, field
from typing import Callable
from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from streaming.init import initialize_spark
from streaming.pipelines.base import BatchPipeline
from streaming.assets.cleaning import strip_non_ascii, trim_whitespace, convert_clock_units, \
    normalize_pcie_interface, empty_to_null
from streaming.assets.extract import infer_manufacturer
from streaming.assets.filtering import filter_integration_test_rows, filter_null
from streaming.schemas import GPU_SPECIFICATION_SCHEMA


def filter_null_tdp(df: DataFrame) -> DataFrame:
    return filter_null(df, "tdp_watts")

def infer_gpu_manufacturer(df: DataFrame) -> DataFrame:
    return infer_manufacturer(df, "model_name", "manufacturer_name")

def convert_core_base_clock(df: DataFrame) -> DataFrame:
    return convert_clock_units(df, "core_base_clock_mhz")

def rename_core_base_clock(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("core_base_clock_mhz", "core_base_clock_ghz")

def convert_core_boost_clock(df: DataFrame) -> DataFrame:
    return convert_clock_units(df, "core_boost_clock_mhz")

def rename_core_boost_clock(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("core_boost_clock_mhz", "core_boost_clock_ghz")


@dataclass
class GPUPipeline(BatchPipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        strip_non_ascii,
        trim_whitespace,
        filter_integration_test_rows,
        empty_to_null,
        filter_null_tdp,
        infer_gpu_manufacturer,
        convert_core_base_clock,
        rename_core_base_clock,
        convert_core_boost_clock,
        rename_core_boost_clock,
        normalize_pcie_interface,
    ])


if __name__ == '__main__':
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    gpu_specifications_pipeline: GPUPipeline = GPUPipeline(
        session=session,
        schema=GPU_SPECIFICATION_SCHEMA,
        config=config_loader.get_open_db_gpu()
    )
    gpu_specifications_pipeline.run()