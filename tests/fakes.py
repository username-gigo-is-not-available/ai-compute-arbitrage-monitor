"""Stand-ins for the network and the Iceberg catalog, shared by the test files."""

import tempfile
from http import HTTPStatus
from pathlib import Path

import requests
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import update_table_metadata


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
