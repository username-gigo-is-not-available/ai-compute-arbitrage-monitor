from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

from refine.schemas.base import META_COLUMNS_SCHEMA

EXCHANGE_RATE_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("from_currency", StringType(), nullable=False),
        StructField("to_currency", StringType(), nullable=False),
        StructField("value", FloatType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
    ])
