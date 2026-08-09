import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from common.classes import Dataset
from common.enums import DataStageType
from common.types import DatasetConfig
from config.storage import GCPStorageConfig
from ingest.models.types import IngestorRecord
from serializers.json_serializer import JsonSerializer


@dataclass
class Ingestor(ABC):
    dataset: Dataset
    config: DatasetConfig
    storage_config: GCPStorageConfig
    name: str = field(init=False)
    logger: logging.Logger = field(init=False)

    def __post_init__(self):
        self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    def parse(self, **kwargs) -> IngestorRecord | None:
        raise NotImplementedError

    def _handle_result(self, data: list[IngestorRecord]) -> None:
        if data:
            self.logger.info(f"{self.name} fetched {len(data)} records")
            self.store(data)
        else:
            self.logger.warning(f"{self.name} returned no data, skipping store.")

    def store(self, data: list[IngestorRecord]) -> None:
        output_path = self.storage_config.directory_path(DataStageType.BRONZE, self.dataset)
        records: list[dict[str, Any]] = JsonSerializer.serialize_batch(data)
        table = pa.Table.from_pylist(records)
        pq.write_to_dataset(table, root_path=output_path)
        self.logger.info(f"Written {len(table)} {self.name} records to {output_path}")


@dataclass
class SyncIngestor(Ingestor):

    @abstractmethod
    def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self._handle_result(self.load())


@dataclass
class AsyncIngestor(Ingestor):

    @abstractmethod
    async def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    async def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self._handle_result(data=await self.load())