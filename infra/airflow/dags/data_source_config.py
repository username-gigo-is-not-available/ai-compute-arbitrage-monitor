from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from ingest.models.enums import DatasetType


@dataclass
class PipelineConfig:
    name: str
    run_fn: Callable
    schedule: str
    start_date: datetime
    dataset_type: DatasetType
    description: str = ""

    # Derived fields
    dag_id: str = field(init=False)

    def __post_init__(self) -> None:
        self.dag_id = f"gpu_arbitrage__{self.name}"

    @property
    def dbt_tag(self) -> str:
        return f"tag:{self.name}"

    @property
    def external_table_selector(self) -> str:
        return f"{self.dataset_type.value.lower()}.{self.name}"

    def entrypoint(self, bucket_name: str):
        return f"gs://{bucket_name}/src/refine/pipelines/{self.name}"




