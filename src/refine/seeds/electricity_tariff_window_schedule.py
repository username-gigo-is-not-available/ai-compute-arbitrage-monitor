import re
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import reduce

from pyspark.sql import DataFrame, Row, functions as F

from common.enums import TariffWindowType
from refine.assets.cleaning import empty_to_null, trim_whitespace
from refine.assets.extraction import extract_valid_from_date
from refine.assets.filtering import keep_latest_per_group
from refine.assets.patterns import SCHEDULE_LOW_TARIFF_HOUR_PAIR_PATTERN, WEEKDAY_WEEKEND_SPLIT
from refine.base import Pipeline


def extract_low_tariff_window_hours(text: str) -> set[int]:
    low_hours = set()
    for s, e in re.findall(SCHEDULE_LOW_TARIFF_HOUR_PAIR_PATTERN, text):
        start, end = int(s[:2]), int(e[:2])
        low_hours.update(range(start, end) if end > start else [*range(start, 24), *range(end)])
    return low_hours


@dataclass
class ElectricityTariffWindowSchedulePipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(
        default_factory=lambda: [trim_whitespace, empty_to_null]
    )

    def generate(self, df: DataFrame) -> DataFrame | None:
        # One hourly grid per tariff version: Bronze keeps every schedule EVN has published, and dbt needs them all
        latest = keep_latest_per_group(extract_valid_from_date(df), group_by="valid_from", order_by="ingested_at")
        versions = latest.select("schedule_text", "valid_from", "ingested_at").collect()
        return reduce(DataFrame.unionByName, [self.hourly_grid(version) for version in versions])

    def hourly_grid(self, version: Row) -> DataFrame:
        weekday_text, _ = version["schedule_text"].split(WEEKDAY_WEEKEND_SPLIT)
        low_hours = extract_low_tariff_window_hours(weekday_text)

        days = self.session.range(1, 8).withColumnRenamed("id", "day_of_week")
        hours = self.session.range(0, 24).withColumnRenamed("id", "start_hour")
        return (
            days.crossJoin(hours)
            .withColumn("end_hour", F.col("start_hour") + 1)
            .withColumn(
                "tariff_window_type",
                F.when(
                    (F.col("day_of_week") == 7) | F.col("start_hour").isin(list(low_hours)),
                    TariffWindowType.LOW.value,
                ).otherwise(TariffWindowType.HIGH.value),
            )
            .withColumn("valid_from", F.lit(version["valid_from"]))
            .withColumn("ingested_at", F.lit(version["ingested_at"]))
            .select("tariff_window_type", "day_of_week", "start_hour", "end_hour", "valid_from", "ingested_at")
        )
