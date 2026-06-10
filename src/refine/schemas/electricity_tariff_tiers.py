from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

from refine.schemas.base import META_COLUMNS_SCHEMA

ELECTRICITY_TARIFF_TIERS_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("tariff_description", StringType(), nullable=False),
        StructField("price_mkd_per_kwh", FloatType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
    ])
