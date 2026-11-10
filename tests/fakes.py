"""Stand-ins for the network and the Iceberg catalog, shared by the test files."""

import tempfile
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path
from typing import Any, TypeVar

import requests
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import update_table_metadata
from pyiceberg.types import NestedField, StringType
from pyspark.sql import DataFrame

from common.classes import Dataset, Snapshot
from common.enums import DatasetName, DatasetType, DataStageType
from config.http import HttpConfig
from config.lakehouse import GCPLakehouseConfig
from ingest.base import Ingestor
from ingest.http_client import HttpClient
from ingest.write_strategy import AppendByDay, BronzeTable
from lakehouse.partition import Partition
from refine.load_strategy import OverwriteByPartition


class FakeResponse:
    """Stands in for requests.Response at the level the ingestors use."""

    def __init__(
        self, status: int = HTTPStatus.OK, text: str = "", json_data: dict | None = None, headers: dict | None = None
    ):
        self.status_code = status
        self.headers = headers or {}
        self.text = text
        self._json_data = json_data or {}
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def raise_for_status(self) -> None:
        if self.status_code != HTTPStatus.OK:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)

    def json(self, **kwargs) -> dict:
        return self._json_data


class FakeCatalog:
    """Keeps real Iceberg table metadata in memory and applies commits with pyiceberg's own update logic.
    Data files are written for real, to a temporary directory."""

    def __init__(self, namespaces: set[str] | None = None):
        self.namespaces = namespaces or set()
        self.created_tables: list[tuple[str, str]] = []
        self.metadata = {}
        self.tmp = tempfile.TemporaryDirectory()
        self.location = Path(self.tmp.name).as_posix()

    def supports_server_side_planning(self) -> bool:
        return False

    def create_namespace_if_not_exists(self, namespace: str) -> None:
        self.namespaces.add(namespace)

    def create_table_if_not_exists(self, identifier, schema, partition_spec) -> Table:
        if identifier not in self.metadata:
            self.created_tables.append(identifier)
            self.metadata[identifier] = new_table_metadata(
                schema, partition_spec, UNSORTED_SORT_ORDER, location=self.location
            )
        return self.load_table(identifier)

    def load_table(self, identifier) -> Table:
        return Table(identifier, self.metadata[identifier], f"{self.location}/metadata.json", PyArrowFileIO(), self)

    def commit_table(self, table, requirements, updates) -> CommitTableResponse:
        self.metadata[table.name()] = update_table_metadata(self.metadata[table.name()], updates)
        return CommitTableResponse(
            metadata=self.metadata[table.name()], metadata_location=f"{self.location}/metadata.json"
        )


IngestorT = TypeVar("IngestorT", bound=Ingestor)


def no_network(url: str, **kwargs: Any) -> FakeResponse:
    raise AssertionError(f"a test tried to reach the network: {url}")


def build_ingestor(
    ingestor_class: type[IngestorT],
    dataset_name: DatasetName,
    dataset_type: DatasetType,
    config: Any,
    send: Callable[..., FakeResponse] = no_network,
    snapshot: Snapshot | None = None,
) -> IngestorT:
    """Placeholder lakehouse and Bronze table, one attempt per request, no waits, no network unless send is given."""
    return ingestor_class(
        dataset=Dataset(dataset_name=dataset_name, dataset_type=dataset_type),
        config=config,
        lakehouse_config=GCPLakehouseConfig(catalog_id="test", project_id="test", warehouse="gs://test"),
        bronze_table=BronzeTable(
            Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False)),
            AppendByDay(column="ingested_at"),
        ),
        http=HttpClient(HttpConfig(timeout_seconds=30, retry_count=1, retry_delay_seconds=0), send=send),
        snapshot=snapshot or Snapshot.from_scheduled_at(None),
    )


class ViewNames:
    """A Silver pipeline's lakehouse_config for Spark temp views, which cannot have catalog.namespace.table names."""

    def spark_table(self, stage: DataStageType, dataset: Dataset) -> str:
        return f"{stage}_{dataset.dataset_name}"


class RecordedWrite(OverwriteByPartition):
    """The real watermark read; the write is recorded instead of sent to the catalog."""

    def __init__(self, partition: Partition) -> None:
        super().__init__(partition)
        self.written: dict[str, list] = {}

    def write(self, df: DataFrame, table_name: str) -> None:
        self.written[table_name] = df.collect()
