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

def normalize_pcie_interface(df: DataFrame, column: str = "interface_type") -> DataFrame:
    """Normalize bare PCIe interface strings to versioned equivalents.

    PCIe x16 -> PCIe 3.0 x16 (pre-versioning standard)
    PCIe x8  -> PCIe 3.0 x8
    PCIe x1  -> PCIe 3.0 x1
    """
    return df.withColumn(
        column,
        F.regexp_replace(F.col(column), r"^PCIe (x\d+)$", "PCIe 3.0 $1")
    )


def convert_clock_units(df: DataFrame, column: str) -> DataFrame:
    """Convert clock unit from MHz to GHz where value > 100."""
    return df.withColumn(
        column,
        F.when(F.col(column) > 100, F.col(column) / 1000)
        .otherwise(F.col(column))
    )
