from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial

from pyspark.sql import DataFrame

from refine.assets.cleaning import empty_to_null, trim_whitespace
from refine.assets.extraction import extract_pattern, extract_valid_from_date
from refine.assets.filtering import keep_latest_per_group
from refine.assets.patterns import KWH_BOUNDS_PATTERN, TARIFF_BLOCK_NUMBER_PATTERN
from refine.base import Pipeline


def extract_tariff_block_number(df: DataFrame) -> DataFrame:
    return extract_pattern(df,
                           source_column="tariff_block_number_text",
                           target_column="tariff_block_number",
                           pattern=TARIFF_BLOCK_NUMBER_PATTERN)


def extract_lower_bound_kwh(df: DataFrame) -> DataFrame:
    return extract_pattern(df,
                           source_column="kwh_boundaries_text",
                           target_column="lower_bound_kwh",
                           pattern=KWH_BOUNDS_PATTERN,
                           group=1)

def extract_upper_bound_kwh(df: DataFrame) -> DataFrame:
    return extract_pattern(df,
                           source_column="kwh_boundaries_text",
                           target_column="upper_bound_kwh",
                           pattern=KWH_BOUNDS_PATTERN,
                           group=2,
                           nullable=True)


@dataclass
class ElectricityTariffBlocksPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        extract_tariff_block_number,
        extract_lower_bound_kwh,
        extract_upper_bound_kwh,
        extract_valid_from_date,
        partial(keep_latest_per_group, group_by="valid_from", order_by="ingested_at")
    ])
