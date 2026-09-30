import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, cast

import pyarrow as pa
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.io.pyarrow import pyarrow_to_schema
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
from config.loader import ConfigLoader
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
        arrow_table = pa.Table.from_pylist(JsonSerializer.serialize_batch(data))
        config = ConfigLoader().get_lakehouse()
        catalog = config.open_catalog()
        namespace = config.namespace(DataStageType.BRONZE, self.dataset)
        table_id = config.table_id(DataStageType.BRONZE, self.dataset)

        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

        if catalog.table_exists(table_id):
            iceberg_table = catalog.load_table(table_id)
        else:
            schema = pyarrow_to_schema(arrow_table.schema)
            iceberg_table = catalog.create_table(
                table_id,
                schema=schema,
                partition_spec=config.partition_spec(self.dataset.dataset_type, schema),
            )

        if self.dataset.dataset_type == DatasetType.SOURCES:
            iceberg_table.append(arrow_table)
        else:
            iceberg_table.overwrite(arrow_table)

        self.logger.info(f"Written {len(arrow_table)} {self.name} records to {namespace}.{table_id[1]}")


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
        self._handle_result(data=await self.load())