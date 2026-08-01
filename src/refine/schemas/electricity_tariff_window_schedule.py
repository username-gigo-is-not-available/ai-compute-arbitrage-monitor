from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SCHEMA = StructType(
    [
        StructField("tariff_window_type", StringType(), nullable=False),
        StructField("day_of_week", IntegerType(), nullable=False),
        StructField("start_hour", IntegerType(), nullable=False),
        StructField("end_hour", IntegerType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False)
    ]
)
