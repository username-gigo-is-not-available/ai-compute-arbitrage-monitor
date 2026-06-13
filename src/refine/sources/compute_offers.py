from dataclasses import dataclass, field
from typing import Callable
from pyspark.sql import DataFrame, SparkSession

from common.classes import Dataset
from common.enums import DataStageType, DatasetType, DatasetName
from config.loader import ConfigLoader
from config.apis.vast_ai import VastAIConfig
from config.storage import GCPStorageConfig
from refine.assets.filtering import deduplicate
from refine.init import initialize_spark
from refine.base import Pipeline
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


def run():
    session: SparkSession = initialize_spark()
    loader: ConfigLoader = ConfigLoader()
    vast_ai_config: VastAIConfig = loader.get_vast_ai()
    storage_config: GCPStorageConfig = loader.get_storage()
    compute_offers: Dataset = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
    if not vast_ai_config.enabled:
        return

    compute_offers_pipeline: ComputeOffersPipeline = ComputeOffersPipeline(
        session=session,
        schema=COMPUTE_OFFER_SCHEMA,
        dataset=compute_offers,
        config=vast_ai_config,
        storage_config=storage_config
    )
    compute_offers_pipeline.run()


if __name__ == "__main__":
    run()
