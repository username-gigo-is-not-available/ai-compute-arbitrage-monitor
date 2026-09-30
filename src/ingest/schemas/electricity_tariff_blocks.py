from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

ELECTRICITY_TARIFF_BLOCKS_BRONZE_SCHEMA = Schema(
    NestedField(field_id=1, name="consumer_category", field_type=StringType(), required=True),
    NestedField(field_id=2, name="tariff_window_type", field_type=StringType(), required=True),
    NestedField(field_id=3, name="tariff_block_number_text", field_type=StringType(), required=True),
    NestedField(field_id=4, name="kwh_boundaries_text", field_type=StringType(), required=True),
    NestedField(field_id=5, name="valid_from_text", field_type=StringType(), required=True),
    NestedField(field_id=6, name="ingested_at", field_type=TimestamptzType(), required=True),
)
