from dataclasses import dataclass
from datetime import datetime, timedelta

from common.enums import DatasetName
from config.datasets import DATASETS

# Shared by every DAG: the dataset DAGs and the marts DAG
DEFAULT_ARGS = {
    "owner": "gigo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}
DAG_TAGS = {"ai", "compute", "arbitrage", "monitor", "capstone"}


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

    # The dataset DAG emits this asset when its tests pass and the marts DAG waits on it, so both must
    # read the name from here: a mismatch fails nowhere, the marts DAG just never runs.
    @property
    def asset_name(self) -> str:
        return f"ai-compute-arbitrage-monitor/{self.dataset_name}"


# One DAG per declared dataset, so the DAG list cannot drift from config.datasets.DATASETS
PIPELINE_CONFIGS: list[PipelineConfig] = [
    PipelineConfig(dataset_name=declaration.name, description=f"{declaration.description} → Bronze → Silver → Gold")
    for declaration in DATASETS.values()
]
