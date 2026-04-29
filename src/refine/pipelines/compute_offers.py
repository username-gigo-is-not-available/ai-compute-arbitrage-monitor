from dataclasses import dataclass, field
from typing import Callable
from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from refine.assets.filtering import deduplicate
from refine.init import initialize_spark
from refine.pipelines.base import Pipeline
from refine.assets.cleaning import strip_non_ascii, trim_whitespace, replace_substring, empty_to_null
from refine.schemas.compute_offers import COMPUTE_OFFER_SCHEMA


def strip_cpu_core_suffix(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="cpu_model_name", current=r"\s+\d+-Core(s)? Processor$", replacement="")


def deduplicate_compute_offers(df: DataFrame) -> DataFrame:
    return deduplicate(df, columns=["offer_id", "offer_type"])


@dataclass
class ComputeOffersPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        strip_non_ascii,
        trim_whitespace,
        empty_to_null,
        strip_cpu_core_suffix,
        deduplicate_compute_offers
    ])


if __name__ == '__main__':
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    compute_offers_pipeline: ComputeOffersPipeline = ComputeOffersPipeline(
        session=session,
        schema=COMPUTE_OFFER_SCHEMA,
        config=config_loader.get_vast_ai(),
    )
    compute_offers_pipeline.run()
