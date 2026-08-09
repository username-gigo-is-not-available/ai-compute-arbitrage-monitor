from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

EXCHANGE_RATE_SCHEMA = StructType(
    [
        StructField("from_currency", StringType(), nullable=False),
        StructField("to_currency", StringType(), nullable=False),
        StructField("value", FloatType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
    ])
