import re
from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from common.classes import Dataset
from common.enums import DatasetType, DatasetName, TariffWindowType
from config.apis.evn import EVNConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from refine.assets.cleaning import trim_whitespace, empty_to_null
from refine.assets.extraction import extract_valid_from_date
from refine.assets.patterns import SCHEDULE_LOW_TARIFF_HOUR_PAIR_PATTERN, WEEKDAY_WEEKEND_SPLIT
from refine.base import Pipeline
from refine.init import initialize_spark
from refine.schemas.base import META_COLUMNS_SCHEMA
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


def get_tariff_window(day: int, hour: int, low_hours: set[int]) -> TariffWindowType:
    if day == 7 or hour in low_hours:
        return TariffWindowType.LOW
    return TariffWindowType.HIGH


@dataclass
class ElectricityTariffWindowSchedulePipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null
    ])

    def generate(self, df: DataFrame) -> DataFrame | None:
        schedule_text: str = df.first()['schedule_text']
        valid_from = extract_valid_from_date(df).first()['valid_from']

        parts = schedule_text.split(WEEKDAY_WEEKEND_SPLIT)

        weekday_text, _ = parts
        low_hours = extract_low_tariff_window_hours(weekday_text)
        rows = [
            {
                "tariff_window": get_tariff_window(day, h, low_hours).value,
                "day_of_week": day,
                "start_hour": h,
                "end_hour": h + 1,
                "valid_from": valid_from
            }
            for day in range(1, 8)
            for h in range(24)
        ]
        meta_field_names = {column.name for column in META_COLUMNS_SCHEMA}
        schema = StructType([f for f in ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SCHEMA.fields if f.name not in meta_field_names])
        return self.session.createDataFrame(rows, schema=schema)


def run():
    session: SparkSession = initialize_spark()
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_window_schedule: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
                                                   dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_window_schedule_pipeline: ElectricityTariffWindowSchedulePipeline = ElectricityTariffWindowSchedulePipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SCHEMA,
        dataset=electricity_tariff_window_schedule,
        config=evn_config,
        storage_config=storage_config,
    )

    electricity_tariff_window_schedule_pipeline.run()


if __name__ == "__main__":
    run()
