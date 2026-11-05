import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pyarrow as pa

from common.classes import Dataset, Snapshot
from common.enums import DataStageType
from common.types import DatasetConfig
from config.lakehouse import GCPLakehouseConfig
from ingest.http_client import HttpClient
from ingest.models.types import IngestorRecord
from ingest.write_strategy import BronzeTable


@dataclass
class Ingestor(ABC):
    dataset: Dataset
    config: DatasetConfig
    lakehouse_config: GCPLakehouseConfig
    bronze_table: BronzeTable
    http: HttpClient  # retries live here, not in the ingestor
    snapshot: Snapshot  # the scheduled day this run is for (ADR-019)
    name: str = field(init=False)
    logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    def handle_result(self, data: list[IngestorRecord]) -> None:
        if not data:
            raise RuntimeError(f"{self.name} fetched no records")
        self.logger.info(f"{self.name} fetched {len(data)} records")
        self.store(data)

    def store(self, data: list[IngestorRecord]) -> None:
        # The setup task has already made the table match its declaration (lakehouse.setup); a table that does not
        # match fails this write
        catalog = self.lakehouse_config.open_catalog()
        arrow_table = pa.Table.from_pylist([r.to_row() for r in data])
        table = catalog.load_table(self.lakehouse_config.table_id(DataStageType.BRONZE, self.dataset))
        self.bronze_table.write(table, arrow_table)
        self.logger.info(f"Written {len(arrow_table)} {self.name} records to Bronze")

    def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self.handle_result(self.load())
