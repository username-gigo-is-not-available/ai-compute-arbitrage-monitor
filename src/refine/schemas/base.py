from pyspark.sql.types import StructField, TimestampType

META_COLUMNS_SCHEMA = [
    StructField("ingested_at", TimestampType(), nullable=True),
    StructField("processed_at", TimestampType(), nullable=True),

]
