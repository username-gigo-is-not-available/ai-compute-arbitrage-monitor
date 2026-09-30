import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from http import HTTPStatus
from typing import Any, cast

import pyarrow as pa
from pyiceberg.schema import Schema
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_fixed,
)

from common.classes import Dataset
from common.enums import DataStageType, DatasetType
from common.types import DatasetConfig
from config.lakehouse import GCPLakehouseConfig
from config.storage import GCPStorageConfig
from ingest.models.types import IngestorRecord


@dataclass
class Ingestor(ABC):
    dataset: Dataset
    config: DatasetConfig
    storage_config: GCPStorageConfig
    lakehouse_config: GCPLakehouseConfig
    bronze_schema: Schema
    name: str = field(init=False)
    logger: logging.Logger = field(init=False)

    def __post_init__(self):
        self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    def parse(self, **kwargs) -> IngestorRecord | None:
        raise NotImplementedError

    def init(self) -> None:
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        catalog = self.lakehouse_config.open_catalog()
        namespace = self.lakehouse_config.namespace(DataStageType.BRONZE, self.dataset)
        table_id = self.lakehouse_config.table_id(DataStageType.BRONZE, self.dataset)
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
        if not catalog.table_exists(table_id):
            catalog.create_table(
                table_id,
                schema=self.bronze_schema,
                partition_spec=self.lakehouse_config.partition_spec(self.dataset.dataset_type, self.bronze_schema),
            )

    def _handle_result(self, data: list[IngestorRecord]) -> None:
        if data:
            self.logger.info(f"{self.name} fetched {len(data)} records")
            self.store(data)
        else:
            self.logger.warning(f"{self.name} returned no data, skipping store.")

    def store(self, data: list[IngestorRecord]) -> None:
        rows = [{k: v.value if isinstance(v, Enum) else v for k, v in r.model_dump().items()} for r in data]
        arrow_table = pa.Table.from_pylist(rows)
        catalog = self.lakehouse_config.open_catalog()
        table = catalog.load_table(self.lakehouse_config.table_id(DataStageType.BRONZE, self.dataset))
        if self.dataset.dataset_type == DatasetType.SOURCES:
            table.append(arrow_table)
        else:
            table.overwrite(arrow_table)
        self.logger.info(f"Written {len(arrow_table)} {self.name} records to Bronze")


@dataclass
class SyncIngestor(Ingestor):

    @abstractmethod
    def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    def retry_sync(self, exc_type):
        return retry(
            stop=stop_after_attempt(self.http_config.retry_count),
            wait=wait_fixed(self.http_config.retry_delay_seconds),
            retry=retry_if_exception_type(exc_type)
            | retry_if_result(lambda resp: resp.status_code != HTTPStatus.OK),
            retry_error_callback=lambda retry_state: retry_state.outcome.result(),
            before_sleep=before_sleep_log(cast(Any, self.logger), logging.WARNING),
        )

    def fetch_sync(self, exc_type, http_call_fn, *args, **kwargs):
        @self.retry_sync(exc_type)
        def fetch():
            resp = http_call_fn(*args, **kwargs)
            if resp.status_code != HTTPStatus.OK:
                resp.close()
            return resp
        return fetch()

    def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self.init()
        self._handle_result(self.load())


@dataclass
class AsyncIngestor(Ingestor):

    @abstractmethod
    async def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    def retry_async(self, exc_type):
        return retry(
            stop=stop_after_attempt(self.http_config.retry_count),
            wait=wait_fixed(self.http_config.retry_delay_seconds),
            retry=retry_if_exception_type(exc_type)
            | retry_if_result(lambda resp: resp.status != HTTPStatus.OK),
            retry_error_callback=lambda retry_state: retry_state.outcome.result(),
            before_sleep=before_sleep_log(cast(Any, self.logger), logging.WARNING),
        )

    async def fetch_async(self, exc_type, http_call_fn, *args, **kwargs):
        @self.retry_async(exc_type)
        async def fetch():
            resp = await http_call_fn(*args, **kwargs)
            if resp.status != HTTPStatus.OK:
                resp.close()
            return resp
        return await fetch()

    async def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self.init()
        self._handle_result(data=await self.load())
