from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from src.common.enums import DatasetType


@dataclass
class PipelineConfig:
    name: str
    ingest_fn: Callable
    refine_fn: Callable
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





