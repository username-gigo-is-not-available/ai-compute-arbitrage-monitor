import logging
from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from common.types import DatasetConfig
from ingest.models.types import IngestorRecord
from serializers.json_serializer import JsonSerializer


class BatchIngestor(ABC):

    def __init__(self, config: DatasetConfig, name: str = None):
        self.name = name or self.__class__.__name__
        self.config = config
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    def parse(self, **kwargs) -> IngestorRecord | None:
        raise NotImplementedError

    def _handle_result(self, data: list[IngestorRecord]) -> None:
        if data:
            self.logger.info(
                f"{self.name} fetched {len(data)} records"
            )
            self.store(data)
        else:
            self.logger.warning(f"{self.name} returned no data, skipping store.")

    def store(self, data: list[IngestorRecord]) -> None:
        output_path: str = self.config.output_path_with_suffix(".parquet")
        records: list[dict[str, Any]] = JsonSerializer.serialize_batch(data)
        table = pa.Table.from_pylist(records)
        pq.write_table(table, output_path)
        self.logger.info(f"Written {len(table)} {self.name} records to {output_path}")



class SyncBatchIngestor(BatchIngestor):

    @abstractmethod
    def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self._handle_result(self.load())


class AsyncBatchIngestor(BatchIngestor):

    @abstractmethod
    async def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    async def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self._handle_result(data=await self.load())


