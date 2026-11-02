from dataclasses import dataclass
from datetime import datetime

from common.enums import DatasetName


# How each dataset is scheduled in Airflow. What the dataset is (type, settings, ingestor, pipeline) lives in the
# registries: config.datasets, ingest.registry, refine.registry. No Airflow import here, so tests can read the list
# in CI.
@dataclass(frozen=True)
class PipelineConfig:
    dataset_name: DatasetName
    schedule: str
    start_date: datetime
    description: str = ""

    @property
    def dag_id(self) -> str:
        return f"ai_compute_arbitrage_monitor__{self.dataset_name}"

    @property
    def dbt_tag(self) -> str:
        return f"tag:{self.dataset_name}"


PIPELINE_CONFIGS: list[PipelineConfig] = [
    PipelineConfig(
        dataset_name=DatasetName.COMPUTE_OFFERS,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="Vast.ai compute offers → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.EXCHANGE_RATES,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="MKD/USD exchange rate → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff tiers → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_BLOCKS,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff blocks → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_FEES,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff fees → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff window schedule → Bronze → Silver → Gold",
    ),
]
