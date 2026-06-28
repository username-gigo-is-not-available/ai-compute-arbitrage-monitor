from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

from refine.schemas.base import META_COLUMNS_SCHEMA

ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("tariff_window", StringType(), nullable=False),
        StructField("day_of_week", IntegerType(), nullable=False),
        StructField("start_hour", IntegerType(), nullable=False),
        StructField("end_hour", IntegerType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False)
    ]
)
