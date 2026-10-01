from abc import ABC, abstractmethod
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.expressions import In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import HourTransform, IdentityTransform


class BronzeWriteStrategy(ABC):
    def __init__(self, column: str) -> None:
        self.column = column

    @abstractmethod
    def partition_spec(self, schema: Schema) -> PartitionSpec:
        raise NotImplementedError

    @abstractmethod
    def write(self, table: Table, arrow_table: pa.Table) -> None:
        raise NotImplementedError


class AppendByHour(BronzeWriteStrategy):
    """Event logs: append-only, partitioned by hour(column) — ingested_at, or snapshot_at for compute_offers (ADR-019)."""

    def __init__(self, column: str = "ingested_at") -> None:
        super().__init__(column)

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=HourTransform(), name=f"{self.column}_hour")
        )

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        table.append(arrow_table)


class OverwriteByPartition(BronzeWriteStrategy):
    """Effective-dated data: identity partition on column; re-scraping a value replaces it, a new value adds a partition."""

    def __init__(self, column: str = "valid_from_text") -> None:
        super().__init__(column)

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=IdentityTransform(), name=self.column)
        )

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        values = set(arrow_table.column(self.column).to_pylist())
        table.overwrite(arrow_table, overwrite_filter=In(self.column, values))


@dataclass(frozen=True)
class BronzeTable:
    """A Bronze table's schema and the strategy that partitions and writes it."""
    schema: Schema
    write_strategy: BronzeWriteStrategy

    def partition_spec(self) -> PartitionSpec:
        return self.write_strategy.partition_spec(self.schema)

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        self.write_strategy.write(table, arrow_table)
