from abc import ABC, abstractmethod
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from lakehouse.partition import Partition


class BronzeWriteStrategy(ABC):
    def __init__(self, partition: Partition) -> None:
        self.partition = partition

    def partition_spec(self, schema: Schema) -> PartitionSpec:
        return self.partition.spec(schema)

    @abstractmethod
    def write(self, table: Table, arrow_table: pa.Table) -> None:
        raise NotImplementedError


class AppendByDay(BronzeWriteStrategy):
    def __init__(self, column: str) -> None:
        super().__init__(Partition(column, DayTransform()))

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        table.append(arrow_table)


@dataclass(frozen=True)
class BronzeTable:
    schema: Schema
    write_strategy: BronzeWriteStrategy

    def partition_spec(self) -> PartitionSpec:
        return self.write_strategy.partition_spec(self.schema)

    def write(self, table: Table, arrow_table: pa.Table) -> None:
        self.write_strategy.write(table, arrow_table)
