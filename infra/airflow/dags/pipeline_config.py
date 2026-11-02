from dataclasses import dataclass
from datetime import datetime

from common.enums import DatasetName
from config.datasets import DATASETS


# How Airflow schedules one dataset. Every dataset runs on the same schedule today; give DatasetDeclaration a
# schedule field once one needs a different one.
@dataclass(frozen=True)
class PipelineConfig:
    dataset_name: DatasetName
    description: str = ""
    schedule: str = "@daily"
    start_date: datetime = datetime(2026, 1, 1)

    @property
    def dag_id(self) -> str:
        return f"ai_compute_arbitrage_monitor__{self.dataset_name}"

    @property
    def dbt_tag(self) -> str:
        return f"tag:{self.dataset_name}"


# One DAG per declared dataset, so the DAG list cannot drift from config.datasets.DATASETS
PIPELINE_CONFIGS: list[PipelineConfig] = [
    PipelineConfig(dataset_name=declaration.name, description=f"{declaration.description} → Bronze → Silver → Gold")
    for declaration in DATASETS.values()
]
