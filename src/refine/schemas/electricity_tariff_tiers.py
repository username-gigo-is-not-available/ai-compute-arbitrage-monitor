from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

from refine.schemas.base import META_COLUMNS_SCHEMA

ELECTRICITY_TARIFF_TIERS_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("label", StringType(), nullable=False),
        StructField("metric", StringType(), nullable=False),
        StructField("value", FloatType(), nullable=False),
        StructField("tariff_description", FloatType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
    ])
