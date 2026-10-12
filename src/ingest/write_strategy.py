from abc import ABC, abstractmethod
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.expressions import In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform


class BronzeWriteStrategy(ABC):
    def __init__(self, column: str) -> None:
        self.column = column

    @abstractmethod
    def partition_spec(self, schema: Schema) -> PartitionSpec:
        raise NotImplementedError

    @abstractmethod
    def write(self, table: Table, arrow_table: pa.Table) -> None:
        raise NotImplementedError


class AppendByDay(BronzeWriteStrategy):

    def __init__(self, column: str) -> None:
        super().__init__(column)

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=DayTransform(), name=f"{self.column}_day") # noqa
        )

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        table.append(arrow_table)


class OverwriteByPartition(BronzeWriteStrategy):

    def __init__(self, column: str = "valid_from_text") -> None:
        super().__init__(column)

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=IdentityTransform(), name=self.column)  # noqa
        )

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        values = set(arrow_table.column(self.column).to_pylist())
        table.overwrite(arrow_table, overwrite_filter=In(self.column, values)) # noqa


@dataclass(frozen=True)
class BronzeTable:
    schema: Schema
    write_strategy: BronzeWriteStrategy

    def partition_spec(self) -> PartitionSpec:
        return self.write_strategy.partition_spec(self.schema)

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        self.write_strategy.write(table, arrow_table)
