from pyspark.sql import functions as F, DataFrame

from refine.assets.patterns import DATE_PATTERN, DATE_FORMAT


def add_processed_at_column(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", F.to_utc_timestamp(F.current_timestamp(), 'UTC'))


def extract_pattern(df: DataFrame,
                    source_column: str,
                    target_column: str,
                    pattern: str,
                    group: int = 1,
                    nullable: bool = False) -> DataFrame:
    extracted = F.regexp_extract(F.col(source_column), pattern, group)
    value = F.when(extracted != "", extracted).otherwise(F.lit(None)) if nullable else extracted
    return df.withColumn(target_column, value)


def extract_valid_from_date(df: DataFrame,
                            date_pattern: str = DATE_PATTERN,
                            date_format: str = DATE_FORMAT) -> DataFrame:
    df = extract_pattern(df,
                         source_column="valid_from_text",
                         target_column="valid_from",
                         pattern=date_pattern,
                         )

    return df.withColumn(
        "valid_from",
        F.to_date(F.col("valid_from"), date_format)
    ).drop("valid_from_text")
