from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window


def filter_null(df: DataFrame, col: str) -> DataFrame:
    return df.filter(F.col(col).isNotNull())

def deduplicate(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.dropDuplicates(columns)

def deduplicate_keep_latest(df: DataFrame, columns: list[str], order_by: str) -> DataFrame:
    window = Window.partitionBy(*columns).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")