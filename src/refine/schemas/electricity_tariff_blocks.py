from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

ELECTRICITY_TARIFF_BLOCKS_SILVER_SCHEMA = StructType(
    [
        StructField("consumer_category", StringType(), nullable=True),
        StructField("tariff_block_number", IntegerType(), nullable=True),
        StructField("tariff_window_type", StringType(), nullable=True),
        StructField("lower_bound_kwh", IntegerType(), nullable=True),
        StructField("upper_bound_kwh", IntegerType(), nullable=True),
        StructField("valid_from", DateType(), nullable=False),
    ])
