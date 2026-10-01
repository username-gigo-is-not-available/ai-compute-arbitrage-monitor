import re
from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.classes import Dataset
from common.enums import DatasetType, DatasetName, TariffWindowType
from config.apis.evn import EVNConfig
from config.loader import ConfigLoader
from refine.write_strategy import OverwriteByPartition
from refine.assets.cleaning import trim_whitespace, empty_to_null
from refine.assets.extraction import extract_valid_from_date
from refine.assets.patterns import SCHEDULE_LOW_TARIFF_HOUR_PAIR_PATTERN, WEEKDAY_WEEKEND_SPLIT
from refine.base import Pipeline
from refine.init import initialize_spark
from refine.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SCHEMA


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
        row = df.first()
        schedule_text = row['schedule_text']
        ingested_at = row['ingested_at']
        valid_from = extract_valid_from_date(df).first()['valid_from']

        parts = schedule_text.split(WEEKDAY_WEEKEND_SPLIT)
        weekday_text, _ = parts
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
            .withColumn("valid_from", F.lit(valid_from))
            .withColumn("ingested_at", F.lit(ingested_at))
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
        schema=ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SCHEMA,
        dataset=electricity_tariff_window_schedule,
        config=evn_config,
        lakehouse_config=loader.get_lakehouse(),
        write_strategy=OverwriteByPartition(),
    )

    electricity_tariff_window_schedule_pipeline.run()


if __name__ == "__main__":
    run()
