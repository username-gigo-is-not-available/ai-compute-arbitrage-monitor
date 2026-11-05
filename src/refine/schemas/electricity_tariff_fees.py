from pyspark.sql.types import DateType, StringType, StructField, StructType

ELECTRICITY_TARIFF_FEES_SILVER_SCHEMA = StructType(
    [
        StructField("consumer_category", StringType(), nullable=True),
        StructField("label", StringType(), nullable=True),
        StructField("metric", StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),
        StructField("valid_from", DateType(), nullable=False),
    ]
)
