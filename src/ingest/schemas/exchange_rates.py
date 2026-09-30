from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

EXCHANGE_RATES_SCHEMA = Schema(
    NestedField(field_id=1, name="from_currency", field_type=StringType(), required=True),
    NestedField(field_id=2, name="to_currency", field_type=StringType(), required=True),
    NestedField(field_id=3, name="value", field_type=DoubleType(), required=True),
    NestedField(field_id=4, name="timestamp", field_type=TimestamptzType(), required=True),
    NestedField(field_id=5, name="ingested_at", field_type=TimestamptzType(), required=True),
)
