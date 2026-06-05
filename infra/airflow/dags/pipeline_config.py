import importlib
import logging
from dataclasses import dataclass
from datetime import datetime
from importlib import util
from importlib._bootstrap import ModuleSpec

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

    def __post_init__(self) -> None:
        for module_path in [self.ingest_module, self.refine_module]:
            try:
                spec: ModuleSpec = importlib.util.find_spec(module_path)
                if spec is None:
                    logging.warning(
                        f"PipelineConfig({self.dataset_name}): '{module_path}' "
                        f"not found — tasks will fail at runtime."
                    )
            except ModuleNotFoundError as e:
                logging.warning(
                    f"PipelineConfig({self.dataset_name}): '{module_path}' "
                    f"cannot be resolved — {e}. Tasks will fail at runtime."
                )

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