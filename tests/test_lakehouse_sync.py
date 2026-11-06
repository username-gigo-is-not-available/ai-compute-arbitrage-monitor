"""lakehouse.sync: a table converges on its declared schema and partition spec (ADR-022)."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import update_table_metadata
from pyiceberg.transforms import DayTransform, HourTransform
from pyiceberg.types import DoubleType, IntegerType, LongType, NestedField, StringType, TimestamptzType

from ingest.schemas.compute_offers import COMPUTE_OFFERS_BRONZE_SCHEMA
from lakehouse.sync import sync_table


class _FakeCatalog:
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


TABLE = ("bronze_sources", "compute_offers")


@pytest.fixture
def catalog():
    catalog = _FakeCatalog()
    yield catalog
    catalog.tmp.cleanup()


class TestSyncCreates:
    def test_existing_namespace_is_kept(self):
        catalog = _FakeCatalog(namespaces={"silver_sources"})
        sync_table(catalog, ("silver_sources", "compute_offers"), COMPUTE_OFFERS_BRONZE_SCHEMA, PartitionSpec())
        assert catalog.namespaces == {"silver_sources"}

    def test_sync_creates_namespace_and_missing_table(self, catalog):
        sync_table(catalog, TABLE, schema=COMPUTE_OFFERS_BRONZE_SCHEMA, partition_spec=PartitionSpec())
        assert catalog.namespaces == {"bronze_sources"}
        assert catalog.created_tables == [TABLE]

    def test_sync_does_not_recreate_existing_table(self, catalog):
        for _ in range(2):
            sync_table(catalog, TABLE, schema=COMPUTE_OFFERS_BRONZE_SCHEMA, partition_spec=PartitionSpec())
        assert catalog.created_tables == [TABLE]


class TestSyncSchemaEvolution:
    """An existing table gains the columns its schema lacks, where the schema puts them."""

    def test_existing_table_gains_missing_columns_in_schema_order(self, catalog):
        # The Bronze table as created before ADR-020, then synced with the schema that adds the slice columns.
        new_columns = {"gpu_fraction_of_machine", "gpu_ids"}
        old = Schema(*[f for f in COMPUTE_OFFERS_BRONZE_SCHEMA.fields if f.name not in new_columns])
        sync_table(catalog, TABLE, schema=old, partition_spec=PartitionSpec())
        sync_table(catalog, TABLE, schema=COMPUTE_OFFERS_BRONZE_SCHEMA, partition_spec=PartitionSpec())
        names = [f.name for f in catalog.load_table(TABLE).schema().fields]
        assert names == [f.name for f in COMPUTE_OFFERS_BRONZE_SCHEMA.fields]

    def test_existing_table_with_the_same_schema_is_left_alone(self, catalog):
        sync_table(catalog, TABLE, schema=COMPUTE_OFFERS_BRONZE_SCHEMA, partition_spec=PartitionSpec())
        schema_id = catalog.load_table(TABLE).schema().schema_id
        sync_table(catalog, TABLE, schema=COMPUTE_OFFERS_BRONZE_SCHEMA, partition_spec=PartitionSpec())
        assert catalog.load_table(TABLE).schema().schema_id == schema_id


OLD_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="price_usd", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="junk", field_type=StringType(), required=False),
    NestedField(field_id=4, name="snapshot_at", field_type=TimestamptzType(), required=False),
)
NEW_SCHEMA = Schema(
    NestedField(field_id=1, name="snapshot_at", field_type=TimestamptzType(), required=False),
    NestedField(field_id=2, name="id", field_type=StringType(), required=False),
    NestedField(field_id=3, name="price_usd", field_type=LongType(), required=False),
    NestedField(field_id=4, name="extra", field_type=DoubleType(), required=False),
)


def _spec(schema: Schema, transform, name: str) -> PartitionSpec:
    source_id = schema.find_field("snapshot_at").field_id
    return PartitionSpec(PartitionField(source_id=source_id, field_id=1000, transform=transform, name=name))


def _sync(catalog: _FakeCatalog, schema: Schema, spec: PartitionSpec) -> Table:
    sync_table(catalog, TABLE, schema=schema, partition_spec=spec)
    return catalog.load_table(TABLE)


def _converge(catalog: _FakeCatalog) -> Table:
    return _sync(catalog, NEW_SCHEMA, _spec(NEW_SCHEMA, DayTransform(), "snapshot_at_day"))


@pytest.fixture
def old_table(catalog) -> _FakeCatalog:
    table = _sync(catalog, OLD_SCHEMA, _spec(OLD_SCHEMA, HourTransform(), "snapshot_at_hour"))
    table.append(
        pa.table(
            {
                "id": ["a", "b"],
                "price_usd": pa.array([1, 2], pa.int32()),
                "junk": ["x", "y"],
                "snapshot_at": [datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 2, tzinfo=UTC)],
            },
            schema=table.schema().as_arrow(),
        )
    )
    return catalog


class TestSyncConverges:
    """Every run makes the table look like its declaration: adds, drops, type widening, order, partition spec (ADR-022)."""

    def test_schema_becomes_the_declared_one(self, old_table):
        table = _converge(old_table)
        assert [(f.name, str(f.field_type)) for f in table.schema().fields] == [
            ("snapshot_at", "timestamptz"),
            ("id", "string"),
            ("price_usd", "long"),
            ("extra", "double"),
        ]

    def test_existing_values_survive_widening_and_new_columns_are_empty(self, old_table):
        rows = _converge(old_table).scan().to_arrow().sort_by("id").to_pylist()
        assert [(r["id"], r["price_usd"], r["extra"]) for r in rows] == [("a", 1, None), ("b", 2, None)]

    def test_partition_spec_becomes_the_declared_one_and_old_files_are_rewritten(self, old_table):
        table = _converge(old_table)
        assert [(f.name, str(f.transform)) for f in table.spec().fields] == [("snapshot_at_day", "day")]
        assert {task.file.spec_id for task in table.scan().plan_files()} == {table.spec().spec_id}
        assert len(table.scan().to_arrow()) == 2

    def test_converged_table_is_left_alone(self, old_table):
        table = _converge(old_table)
        before = (table.schema().schema_id, table.spec().spec_id, table.current_snapshot().snapshot_id)
        table = _converge(old_table)
        assert (table.schema().schema_id, table.spec().spec_id, table.current_snapshot().snapshot_id) == before


def test_rows_with_an_undeclared_column_fail_and_the_table_is_unchanged(catalog):
    # Ingest no longer syncs (#41): rows with a column the table lacks fail the write and change nothing.
    table_id = ("bronze_sources", "exchange_rates")
    synced = Schema(NestedField(field_id=1, name="rate", field_type=DoubleType(), required=False))
    sync_table(catalog, table_id, schema=synced, partition_spec=PartitionSpec())
    rows = pa.table({"rate": pa.array([61.5], pa.float64()), "quote": ["MKD"]})

    with pytest.raises(ValueError):
        catalog.load_table(table_id).append(rows)

    table = catalog.load_table(table_id)
    assert [f.name for f in table.schema().fields] == ["rate"]
    assert table.current_snapshot() is None
