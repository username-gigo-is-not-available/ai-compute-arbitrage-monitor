from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

EXCHANGE_RATES_SILVER_SCHEMA = StructType(
    [
        StructField("from_currency", StringType(), nullable=False),
        StructField("to_currency", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
    ]
)
