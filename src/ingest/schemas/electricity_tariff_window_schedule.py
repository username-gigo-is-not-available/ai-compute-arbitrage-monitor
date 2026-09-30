from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SCHEMA = Schema(
    NestedField(field_id=1, name="schedule_text", field_type=StringType(), required=True),
    NestedField(field_id=2, name="valid_from_text", field_type=StringType(), required=True),
    NestedField(field_id=3, name="ingested_at", field_type=TimestamptzType(), required=True),
)
