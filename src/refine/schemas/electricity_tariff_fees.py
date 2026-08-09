from pyspark.sql.types import StructType, StructField, StringType, DateType

ELECTRICITY_TARIFF_FEES_SCHEMA = StructType(
    [
        StructField("consumer_category", StringType(), nullable=True),
        StructField("label", StringType(), nullable=True),
        StructField("metric", StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),
        StructField("valid_from", DateType(), nullable=False),
    ])
