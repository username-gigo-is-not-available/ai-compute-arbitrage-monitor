from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

ELECTRICITY_TARIFF_FEES_BRONZE_SCHEMA = Schema(
    NestedField(field_id=1, name="consumer_category", field_type=StringType(), required=False),
    NestedField(field_id=2, name="label", field_type=StringType(), required=False),
    NestedField(field_id=3, name="metric", field_type=StringType(), required=False),
    NestedField(field_id=4, name="value", field_type=StringType(), required=False),
    NestedField(field_id=5, name="valid_from_text", field_type=StringType(), required=False),
    NestedField(field_id=6, name="ingested_at", field_type=TimestamptzType(), required=False),
)
