from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window


def keep_latest_per_group(df: DataFrame, group_by: str, order_by: str) -> DataFrame:
    window = Window.partitionBy(group_by)
    return df.withColumn("_max", F.max(order_by).over(window)).filter(F.col(order_by) == F.col("_max")).drop("_max")

def deduplicate_keep_latest(df: DataFrame, columns: list[str], order_by: str) -> DataFrame:
    window = Window.partitionBy(*columns).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")