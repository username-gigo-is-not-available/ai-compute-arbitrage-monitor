from pyspark.sql.types import StructField, TimestampType, StructType

SILVER_META_COLUMNS_SCHEMA = StructType([
    StructField("ingested_at", TimestampType(), nullable=True),
    StructField("processed_at", TimestampType(), nullable=True),

])
