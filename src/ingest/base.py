import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Generic, TypeVar

import pyarrow as pa

from common.classes import Dataset, Snapshot
from common.enums import DataStageType
from common.types import DatasetConfig
from config.lakehouse import GCPLakehouseConfig
from ingest.http_client import HttpClient
from ingest.models.base import BaseRecord
from ingest.write_strategy import BronzeTable

ConfigT = TypeVar("ConfigT", bound=DatasetConfig)
RecordT = TypeVar("RecordT", bound=BaseRecord)


# Each ingestor names its config and record type, e.g. Ingestor[VastAIConfig, VastAIOffer].
@dataclass
class Ingestor(ABC, Generic[ConfigT, RecordT]):
    dataset: Dataset
    config: ConfigT
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
    def load(self) -> list[RecordT]:
        raise NotImplementedError

    def handle_result(self, data: list[RecordT]) -> None:
        if not data:
            raise RuntimeError(f"{self.name} fetched no records")
        self.logger.info(f"{self.name} fetched {len(data)} records")
        self.store(data)

    def store(self, data: list[RecordT]) -> None:
        # The setup task has already made the table match its declaration (lakehouse.setup); a table that does not
        # match fails this write
        catalog = self.lakehouse_config.open_catalog()
        table = catalog.load_table(self.lakehouse_config.table_id(DataStageType.BRONZE, self.dataset))
        rows = [r.to_row() for r in data]
        schema = table.schema().as_arrow()
        # from_pylist with a schema drops keys it does not name; a record field the table lacks must fail instead
        undeclared = {column for row in rows for column in row} - set(schema.names)
        if undeclared:
            raise ValueError(f"{self.name} records have columns the Bronze table lacks: {sorted(undeclared)}")
        # Types come from the table: inferred from the values, a column that is None in every record is a null
        # type, which Iceberg v2 cannot store
        self.bronze_table.write(table, pa.Table.from_pylist(rows, schema=schema))
        self.logger.info(f"Written {len(rows)} {self.name} records to Bronze")

    def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self.handle_result(self.load())
