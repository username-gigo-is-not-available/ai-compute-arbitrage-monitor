from dataclasses import dataclass, field

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform, Transform


@dataclass(frozen=True)
class Partition:
    column: str
    transform: Transform = field(default_factory=IdentityTransform)

    def spec(self, schema: Schema) -> PartitionSpec:
        field_id = schema.find_field(self.column).field_id
        name = f"{self.column}_day" if isinstance(self.transform, DayTransform) else self.column
        return PartitionSpec(
            PartitionField(source_id=field_id, field_id=1000, transform=self.transform, name=name) # noqa
        )
