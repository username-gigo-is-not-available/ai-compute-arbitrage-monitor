from abc import ABC, abstractmethod

import pyarrow as pa
from pyiceberg.expressions import In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import HourTransform, IdentityTransform


class BronzeWriteStrategy(ABC):
    @abstractmethod
    def partition_spec(self, schema):
        raise NotImplementedError

    @abstractmethod
    def write(self, table, arrow_table: pa.Table) -> None:
        raise NotImplementedError


class AppendByHour(BronzeWriteStrategy):
    """Sources: append-only, partitioned by hour(column) — ingested_at, or snapshot_at for compute_offers (ADR-019)."""

    def __init__(self, column: str = "ingested_at") -> None:
        self.column = column

    def partition_spec(self, schema):
        field_id = schema.find_field(self.column).field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=HourTransform(), name=f"{self.column}_hour")
        )

    def write(self, table, arrow_table: pa.Table) -> None:
        table.append(arrow_table)


class OverwriteByValidFrom(BronzeWriteStrategy):
    """Seeds: overwrite by valid_from partition; re-scraping same date replaces, new date appends a new partition."""

    def partition_spec(self, schema):
        field_id = schema.find_field("valid_from_text").field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=IdentityTransform(), name="valid_from_text")
        )

    def write(self, table, arrow_table: pa.Table) -> None:
        valid_from_values = set(arrow_table.column("valid_from_text").to_pylist())
        table.overwrite(arrow_table, overwrite_filter=In("valid_from_text", valid_from_values))
