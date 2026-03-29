from pyspark.sql import functions as F, Column
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType


def filter_null(df: DataFrame, col: str) -> DataFrame:
    return df.filter(F.col(col).isNotNull())

def deduplicate(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.dropDuplicates(columns)