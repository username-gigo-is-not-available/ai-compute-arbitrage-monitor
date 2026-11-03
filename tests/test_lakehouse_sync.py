"""Unit tests for lakehouse.sync: a table converges on its declared schema and partition spec (ADR-022).

Under pytest:   uv run --extra dev pytest tests/test_lakehouse_sync.py
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, UTC
from pathlib import Path

import pyarrow as pa
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
            self.metadata[identifier] = new_table_metadata(schema, partition_spec, UNSORTED_SORT_ORDER,
                                                           location=self.location)
        return self.load_table(identifier)

    def load_table(self, identifier) -> Table:
        return Table(identifier, self.metadata[identifier], f"{self.location}/metadata.json", PyArrowFileIO(), self)

    def commit_table(self, table, requirements, updates) -> CommitTableResponse:
        self.metadata[table.name()] = update_table_metadata(self.metadata[table.name()], updates)
        return CommitTableResponse(metadata=self.metadata[table.name()], metadata_location=f"{self.location}/metadata.json")


class TestSyncCreates(unittest.TestCase):

    def test_existing_namespace_is_kept(self):
        catalog = _FakeCatalog(namespaces={"silver_sources"})
        sync_table(catalog, ("silver_sources", "compute_offers"), COMPUTE_OFFERS_BRONZE_SCHEMA, PartitionSpec())
        self.assertEqual(catalog.namespaces, {"silver_sources"})

    def test_sync_creates_namespace_and_missing_table(self):
        catalog = _FakeCatalog()
        sync_table(catalog, ("bronze_sources", "compute_offers"), schema=COMPUTE_OFFERS_BRONZE_SCHEMA,
                               partition_spec=PartitionSpec())
        self.assertEqual(catalog.namespaces, {"bronze_sources"})
        self.assertEqual(catalog.created_tables, [("bronze_sources", "compute_offers")])

    def test_sync_does_not_recreate_existing_table(self):
        catalog = _FakeCatalog()
        for _ in range(2):
            sync_table(catalog, ("bronze_sources", "compute_offers"), schema=COMPUTE_OFFERS_BRONZE_SCHEMA,
                                   partition_spec=PartitionSpec())
        self.assertEqual(catalog.created_tables, [("bronze_sources", "compute_offers")])

class TestSyncSchemaEvolution(unittest.TestCase):
    """An existing table gains the columns its schema lacks, where the schema puts them."""
    TABLE = ("bronze_sources", "compute_offers")

    def setUp(self):
        self.catalog = _FakeCatalog()

    def _sync(self, schema: Schema) -> None:
        sync_table(self.catalog, ("bronze_sources", "compute_offers"), schema=schema,
                               partition_spec=PartitionSpec())

    def test_existing_table_gains_missing_columns_in_schema_order(self):
        # The Bronze table as created before ADR-020, then synced with the schema that adds the slice columns.
        new_columns = {"gpu_fraction_of_machine", "gpu_ids"}
        self._sync(Schema(*[f for f in COMPUTE_OFFERS_BRONZE_SCHEMA.fields if f.name not in new_columns]))
        self._sync(COMPUTE_OFFERS_BRONZE_SCHEMA)
        names = [f.name for f in self.catalog.load_table(self.TABLE).schema().fields]
        self.assertEqual(names, [f.name for f in COMPUTE_OFFERS_BRONZE_SCHEMA.fields])

    def test_existing_table_with_the_same_schema_is_left_alone(self):
        self._sync(COMPUTE_OFFERS_BRONZE_SCHEMA)
        schema_id = self.catalog.load_table(self.TABLE).schema().schema_id
        self._sync(COMPUTE_OFFERS_BRONZE_SCHEMA)
        self.assertEqual(self.catalog.load_table(self.TABLE).schema().schema_id, schema_id)


class TestSyncConverges(unittest.TestCase):
    """Every run makes the table look like its declaration: adds, drops, type widening, order, partition spec (ADR-022)."""
    TABLE = ("bronze_sources", "compute_offers")
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

    def setUp(self):
        self.catalog = _FakeCatalog()

    def tearDown(self):
        self.catalog.tmp.cleanup()

    @staticmethod
    def _spec(schema: Schema, transform, name: str) -> PartitionSpec:
        source_id = schema.find_field("snapshot_at").field_id
        return PartitionSpec(PartitionField(source_id=source_id, field_id=1000, transform=transform, name=name))

    def _sync(self, schema: Schema, spec: PartitionSpec) -> Table:
        sync_table(self.catalog, ("bronze_sources", "compute_offers"), schema=schema, partition_spec=spec)
        return self.catalog.load_table(self.TABLE)

    def _old_table_with_rows(self) -> None:
        table = self._sync(self.OLD_SCHEMA, self._spec(self.OLD_SCHEMA, HourTransform(), "snapshot_at_hour"))
        table.append(pa.table({
            "id": ["a", "b"],
            "price_usd": pa.array([1, 2], pa.int32()),
            "junk": ["x", "y"],
            "snapshot_at": [datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 2, tzinfo=UTC)],
        }, schema=table.schema().as_arrow()))

    def _converge(self) -> Table:
        return self._sync(self.NEW_SCHEMA, self._spec(self.NEW_SCHEMA, DayTransform(), "snapshot_at_day"))

    def test_schema_becomes_the_declared_one(self):
        self._old_table_with_rows()
        table = self._converge()
        self.assertEqual([(f.name, str(f.field_type)) for f in table.schema().fields],
                         [("snapshot_at", "timestamptz"), ("id", "string"), ("price_usd", "long"), ("extra", "double")])

    def test_existing_values_survive_widening_and_new_columns_are_empty(self):
        self._old_table_with_rows()
        rows = self._converge().scan().to_arrow().sort_by("id").to_pylist()
        self.assertEqual([(r["id"], r["price_usd"], r["extra"]) for r in rows], [("a", 1, None), ("b", 2, None)])

    def test_partition_spec_becomes_the_declared_one_and_old_files_are_rewritten(self):
        self._old_table_with_rows()
        table = self._converge()
        self.assertEqual([(f.name, str(f.transform)) for f in table.spec().fields], [("snapshot_at_day", "day")])
        self.assertEqual({task.file.spec_id for task in table.scan().plan_files()}, {table.spec().spec_id})
        self.assertEqual(len(table.scan().to_arrow()), 2)

    def test_converged_table_is_left_alone(self):
        self._old_table_with_rows()
        table = self._converge()
        schema_id, spec_id, snapshot_id = table.schema().schema_id, table.spec().spec_id, table.current_snapshot().snapshot_id
        table = self._converge()
        self.assertEqual((table.schema().schema_id, table.spec().spec_id, table.current_snapshot().snapshot_id),
                         (schema_id, spec_id, snapshot_id))


class TestWriteBeforeSync(unittest.TestCase):
    """Ingest no longer syncs (#41): rows with a column the table lacks fail the write and change nothing."""
    TABLE = ("bronze_sources", "exchange_rates")
    SYNCED = Schema(NestedField(field_id=1, name="rate", field_type=DoubleType(), required=False))

    def setUp(self):
        self.catalog = _FakeCatalog()
        sync_table(self.catalog, self.TABLE, schema=self.SYNCED, partition_spec=PartitionSpec())

    def tearDown(self):
        self.catalog.tmp.cleanup()

    def test_rows_with_an_undeclared_column_fail_and_the_table_is_unchanged(self):
        table = self.catalog.load_table(self.TABLE)
        rows = pa.table({"rate": pa.array([61.5], pa.float64()), "quote": ["MKD"]})

        with self.assertRaises(ValueError):
            table.append(rows)

        table = self.catalog.load_table(self.TABLE)
        self.assertEqual([f.name for f in table.schema().fields], ["rate"])
        self.assertIsNone(table.current_snapshot())


if __name__ == "__main__":
    unittest.main()
