from dataclasses import field, dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from config.loader import SilverConfigLoader
from refine.assets.filtering import deduplicate
from refine.init import initialize_spark
from refine.pipelines.base import Pipeline
from refine.assets.cleaning import trim_whitespace, empty_to_null, parse_valid_from
from refine.schemas.electricity_tariff_schedule import ELECTRICITY_TARIFF_SCHEDULE_SCHEMA


def deduplicate_electricity_tariffs_schedule(df: DataFrame) -> DataFrame:
    return deduplicate(df, columns=['day_of_week', 'start_hour', 'end_hour', 'tariff_type', 'valid_from'])


@dataclass
class ElectricityTariffsSchedulePipeline(Pipeline):
    transform_steps: list[Callable[[DataFrame], DataFrame]] = field(default_factory=lambda: [
        trim_whitespace,
        empty_to_null,
        parse_valid_from,
        deduplicate_electricity_tariffs_schedule
    ])


def run():
    session: SparkSession = initialize_spark()
    config_loader: SilverConfigLoader = SilverConfigLoader()
    electricity_tariffs_schedule_pipeline: ElectricityTariffsSchedulePipeline = ElectricityTariffsSchedulePipeline(
        session=session,
        schema=ELECTRICITY_TARIFF_SCHEDULE_SCHEMA,
        config=config_loader.get_evn()
    )

    electricity_tariffs_schedule_pipeline.run()


if __name__ == "__main__":
    run()
