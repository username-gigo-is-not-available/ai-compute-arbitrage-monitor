from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType


def string_columns(df: DataFrame) -> list[str]:
    return [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]


def strip_non_ascii(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Strip non-ASCII encoding artifacts e.g. Intel® Pentium®"""
    for column in columns or string_columns(df):
        df = df.withColumn(column, F.regexp_replace(F.col(column), r"[^\x00-\x7F]+", ""))
    return df


def trim_whitespace(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    for column in columns or string_columns(df):
        df = df.withColumn(column, F.trim(F.col(column)))
    return df


def replace_substring(df: DataFrame, column: str, current: str, replacement: str) -> DataFrame:
    return df.withColumn(column, F.regexp_replace(F.col(column), current, replacement))


def empty_to_null(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    for column in columns or string_columns(df):
        df = df.withColumn(column, F.when(F.col(column) != "", F.col(column)).otherwise(F.lit(None)))
    return df
