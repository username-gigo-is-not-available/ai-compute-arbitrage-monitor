from pyspark.sql import functions as F, Column

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType


def strip_non_ascii(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Strip non-ASCII encoding artifacts e.g. Intel® Pentium®"""
    columns: list[str] = columns or [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for column in columns:
        df = df.withColumn(column, F.regexp_replace(F.col(column), r"[^\x00-\x7F]+", ""))
    return df


def trim_whitespace(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    columns: list[str] = columns or [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for column in columns:
        df = df.withColumn(column, F.trim(F.col(column)))
    return df


def replace_substring(df: DataFrame, column: str, current: str, replacement: str) -> DataFrame:
    return df.withColumn(column, F.regexp_replace(F.col(column), current, replacement))


def parse_date(df: DataFrame, column: str, date_format: str) -> DataFrame:
    return df.withColumn(column, F.to_date(F.col(column), date_format))

def empty_to_null(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    columns: list[str] = columns or [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for column in columns:
        df = df.withColumn(column, F.when(F.col(column) != "", F.col(column)).otherwise(F.lit(None)))
    return df

def parse_valid_from(df: DataFrame) -> DataFrame:
    return parse_date(df, column="valid_from", date_format="dd.MM.yyyy")
