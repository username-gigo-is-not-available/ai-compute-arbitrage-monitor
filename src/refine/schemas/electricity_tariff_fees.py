from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from refine.schemas.base import META_COLUMNS_SCHEMA

ELECTRICITY_TARIFF_FEES_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("consumer_category", StringType(), nullable=True),
        StructField("label", StringType(), nullable=True),
        StructField("metric", StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),
        StructField("valid_from", DateType(), nullable=False),
    ])
