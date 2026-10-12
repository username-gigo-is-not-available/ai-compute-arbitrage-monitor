import re
from dataclasses import field, dataclass
from functools import reduce
from typing import Callable

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from common.classes import Dataset
from common.enums import DatasetType, DatasetName, TariffWindowType
from config.apis.evn import EVNConfig
from config.loader import ConfigLoader
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.assets.cleaning import trim_whitespace, empty_to_null
from refine.assets.extraction import extract_valid_from_date
from refine.assets.patterns import SCHEDULE_LOW_TARIFF_HOUR_PAIR_PATTERN, WEEKDAY_WEEKEND_SPLIT
from refine.base import Pipeline
from refine.init import initialize_spark
from refine.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA


def extract_low_tariff_window_hours(text: str) -> set[int]:
    low_hours = set()
    for s, e in re.findall(SCHEDULE_LOW_TARIFF_HOUR_PAIR_PATTERN, text):
        start, end = int(s[:2]), int(e[:2])
        if end > start:
            hours = range(start, end)
        else:
            hours = list(range(start, 24)) + list(range(0, end))
        low_hours.update(hours)
    return low_hours



@dataclass
class ElectricityTariffWindowSchedulePipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null
    ])

    def generate(self, df: DataFrame) -> DataFrame | None:
        # One hourly grid per tariff version: Bronze keeps every schedule EVN has published, and dbt needs them all
        versions = extract_valid_from_date(df).select("schedule_text", "valid_from", "ingested_at").collect()
        return reduce(DataFrame.unionByName, [self.hourly_grid(version) for version in versions])

    def hourly_grid(self, version: Row) -> DataFrame:
        weekday_text, _ = version["schedule_text"].split(WEEKDAY_WEEKEND_SPLIT)
        low_hours = extract_low_tariff_window_hours(weekday_text)

        days = self.session.range(1, 8).withColumnRenamed("id", "day_of_week")
        hours = self.session.range(0, 24).withColumnRenamed("id", "start_hour")
        return (
            days.crossJoin(hours)
            .withColumn("end_hour", F.col("start_hour") + 1)
            .withColumn("tariff_window_type", F.when(
                (F.col("day_of_week") == 7) | F.col("start_hour").isin(list(low_hours)),
                TariffWindowType.LOW.value,
            ).otherwise(TariffWindowType.HIGH.value))
            .withColumn("valid_from", F.lit(version["valid_from"]))
            .withColumn("ingested_at", F.lit(version["ingested_at"]))
            .select("tariff_window_type", "day_of_week", "start_hour", "end_hour", "valid_from", "ingested_at")
        )


def run():
    session: SparkSession = initialize_spark()
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    electricity_tariff_window_schedule: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
                                                   dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_window_schedule_pipeline: ElectricityTariffWindowSchedulePipeline = ElectricityTariffWindowSchedulePipeline(
        session=session,
        dataset=electricity_tariff_window_schedule,
        config=evn_config,
        lakehouse_config=loader.get_lakehouse(),
        silver_table=SilverTable(ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA, OverwriteByPartition()),
    )

    electricity_tariff_window_schedule_pipeline.run()


if __name__ == "__main__":
    run()
