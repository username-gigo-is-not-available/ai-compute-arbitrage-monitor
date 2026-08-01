from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

ELECTRICITY_TARIFF_TIERS_SCHEMA = StructType(
    [
        StructField("consumer_category", StringType(), nullable=False),
        StructField("label", StringType(), nullable=False),
        StructField("metric", StringType(), nullable=False),
        StructField("value", FloatType(), nullable=False),
        StructField("tariff_tier", StringType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
    ])
