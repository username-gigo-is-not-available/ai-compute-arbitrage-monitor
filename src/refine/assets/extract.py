from pyspark.sql import functions as F, DataFrame


def add_processed_at_column(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", F.to_utc_timestamp(F.current_timestamp(), 'UTC'))
