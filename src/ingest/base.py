import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, cast

import pyarrow as pa
import requests
from pyiceberg.catalog import Catalog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    retry_if_result,
    RetryCallState,
    stop_after_attempt,
)

from common.classes import Dataset
from common.enums import DataStageType
from common.types import DatasetConfig
from config.http import HttpConfig
from config.lakehouse import GCPLakehouseConfig
from ingest.models.types import IngestorRecord
from ingest.write_strategy import BronzeTable

ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]


@dataclass
class Ingestor(ABC):
    dataset: Dataset
    config: DatasetConfig
    lakehouse_config: GCPLakehouseConfig
    bronze_table: BronzeTable
    http_config: HttpConfig
    name: str = field(init=False)
    logger: logging.Logger = field(init=False)
    catalog: Catalog | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    def load(self) -> list[IngestorRecord]:
        raise NotImplementedError

    def should_retry(self, resp: Any) -> bool:
        return resp.status_code != HTTPStatus.OK and not self.http_config.retry_after_exceeds_budget(resp.headers)

    def wait_before_retry(self, retry_state: RetryCallState) -> int:
        if retry_state.outcome.failed:
            return self.http_config.retry_delay_seconds
        return self.http_config.retry_wait_seconds(retry_state.outcome.result().headers)

    @staticmethod
    def last_outcome(retry_state: RetryCallState) -> Any:
        return retry_state.outcome.result()

    def retry_policy(self, exception_type: ExceptionTypes) -> Any:
        return retry(
            stop=stop_after_attempt(self.http_config.retry_count),
            wait=self.wait_before_retry,
            retry=retry_if_exception_type(exception_type) | retry_if_result(self.should_retry),
            retry_error_callback=self.last_outcome,
            before_sleep=before_sleep_log(cast(Any, self.logger), logging.WARNING),
        )

    def ensure_bronze_table(self) -> None:
        self.catalog = self.lakehouse_config.open_catalog()
        self.lakehouse_config.ensure_table(
            catalog=self.catalog,
            stage=DataStageType.BRONZE,
            dataset=self.dataset,
            schema=self.bronze_table.schema,
            partition_spec=self.bronze_table.partition_spec(),
        )

    def handle_result(self, data: list[IngestorRecord]) -> None:
        if not data:
            raise RuntimeError(f"{self.name} fetched no records")
        self.logger.info(f"{self.name} fetched {len(data)} records")
        self.store(data)

    def store(self, data: list[IngestorRecord]) -> None:
        catalog = self.catalog or self.lakehouse_config.open_catalog()
        arrow_table = pa.Table.from_pylist([r.to_row() for r in data])
        table = catalog.load_table(self.lakehouse_config.table_id(DataStageType.BRONZE, self.dataset))
        self.bronze_table.write(table, arrow_table)
        self.logger.info(f"Written {len(arrow_table)} {self.name} records to Bronze")

    def fetch(self, url: str, **kwargs: Any) -> requests.Response:
        @self.retry_policy(requests.RequestException)
        def attempt() -> requests.Response:
            resp = requests.get(url, timeout=self.http_config.timeout_seconds, **kwargs)
            if resp.status_code != HTTPStatus.OK:
                resp.close()
            return resp

        response = attempt()
        response.raise_for_status()
        return response

    def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        self.ensure_bronze_table()
        self.handle_result(self.load())

