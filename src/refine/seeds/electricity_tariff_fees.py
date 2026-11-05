from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial

from pyspark.sql import DataFrame

from refine.assets.cleaning import empty_to_null, replace_substring, trim_whitespace
from refine.assets.extraction import extract_valid_from_date
from refine.assets.filtering import keep_latest_per_group
from refine.base import Pipeline


def replace_decimal_separator(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="value", current=",", replacement=".")


def standardize_currency_symbol(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="metric", current="денари", replacement="MKD")


def standardize_label(df: DataFrame) -> DataFrame:
    return replace_substring(df, column="label", current="Активна ел. енергија",
                             replacement="Активна електрична енергија")


@dataclass
class ElectricityTariffFeesPipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        replace_decimal_separator,
        standardize_currency_symbol,
        extract_valid_from_date,
        partial(keep_latest_per_group, group_by="valid_from", order_by="ingested_at"),
        standardize_label
    ])
