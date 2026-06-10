
from dataclasses import dataclass
from datetime import datetime

from common.enums import DatasetName, DatasetType


@dataclass(frozen=True)
class PipelineConfig:
    dataset_name: DatasetName
    dataset_type: DatasetType
    schedule: str
    start_date: datetime
    ingest_module_base: str = "ingest"
    refine_module_base: str = "refine"
    description: str = ""

    @property
    def dag_id(self) -> str:
        return f"ai_compute_arbitrage_monitor__{self.dataset_name}"

    @property
    def dbt_tag(self) -> str:
        return f"tag:{self.dataset_name}"

    @property
    def external_table_selector(self) -> str:
        return f"{self.dataset_type}.{self.dataset_name}"

    @property
    def ingest_module(self) -> str:
        return f"{self.ingest_module_base}.{self.dataset_type}.{self.dataset_name}"

    @property
    def ingest_uri(self) -> str:
        return f"{self.ingest_module_base}/{self.dataset_type}/{self.dataset_name}.py"

    @property
    def refine_module(self) -> str:
        return f"{self.refine_module_base}.{self.dataset_type}.{self.dataset_name}"

    @property
    def refine_uri(self) -> str:
        return f"{self.refine_module_base}/{self.dataset_type}/{self.dataset_name}.py"