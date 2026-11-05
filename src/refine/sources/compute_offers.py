from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import DataFrame

from refine.assets.cleaning import empty_to_null, replace_substring, strip_non_ascii, trim_whitespace
from refine.assets.filtering import deduplicate_keep_latest, keep_latest_per_group
from refine.base import Pipeline


def strip_cpu_core_suffix(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="cpu_model_name", current=r"\s+\d+-Core(s)? Processor$", replacement="")


def deduplicate_compute_offers(df: DataFrame) -> DataFrame:
    df = keep_latest_per_group(df, group_by="snapshot_at", order_by="ingested_at")
    return deduplicate_keep_latest(df, columns=["offer_id", "snapshot_at"], order_by="ingested_at")


@dataclass
class ComputeOffersPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(
        default_factory=lambda: [
            strip_non_ascii,
            trim_whitespace,
            empty_to_null,
            strip_cpu_core_suffix,
            deduplicate_compute_offers,
        ]
    )
