from abc import ABC, abstractmethod

import pyarrow as pa


class BronzeWriteStrategy(ABC):
    @abstractmethod
    def partition_spec(self, schema):
        raise NotImplementedError

    @abstractmethod
    def write(self, table, arrow_table: pa.Table) -> None:
        raise NotImplementedError


class AppendByHour(BronzeWriteStrategy):
    """Sources: append-only, partitioned by hour(ingested_at)."""

    def partition_spec(self, schema):
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import HourTransform

        field_id = schema.find_field("ingested_at").field_id
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=HourTransform(), name="ingested_at_hour")
        )

    def write(self, table, arrow_table: pa.Table) -> None:
        table.append(arrow_table)


class FullOverwrite(BronzeWriteStrategy):
    """Seeds: full table overwrite, unpartitioned."""

    def partition_spec(self, schema):
        from pyiceberg.partitioning import PartitionSpec

        return PartitionSpec()

    def write(self, table, arrow_table: pa.Table) -> None:
        table.overwrite(arrow_table)
