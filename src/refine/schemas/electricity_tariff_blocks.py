from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

ELECTRICITY_TARIFF_BLOCKS_SCHEMA = StructType(
    [
        StructField("consumer_category", StringType(), nullable=True),
        StructField("block_number", IntegerType(), nullable=True),
        StructField("tariff_window", StringType(), nullable=True),
        StructField("lower_bound_kwh", IntegerType(), nullable=True),
        StructField("upper_bound_kwh", IntegerType(), nullable=True),
        StructField("valid_from", DateType(), nullable=False),
    ])
