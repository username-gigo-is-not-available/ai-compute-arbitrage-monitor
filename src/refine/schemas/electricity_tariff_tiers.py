from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA = StructType(
    [
        StructField("consumer_category", StringType(), nullable=False),
        StructField("label", StringType(), nullable=False),
        StructField("metric", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("tariff_tier", StringType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
    ])
