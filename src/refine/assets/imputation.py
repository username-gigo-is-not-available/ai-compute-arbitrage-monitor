from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def fill_from_group(df: DataFrame, group_by: list[str], fill_column: str) -> DataFrame:
    """Fill nulls with the group's one non-null value; row order does not matter."""
    conflicts = df.groupBy(*group_by).agg(F.countDistinct(fill_column).alias("values")).filter(F.col("values") > 1)
    if not conflicts.isEmpty():
        raise ValueError(f"{fill_column} has more than one value per group, so the fill is ambiguous: "
                         f"{[row.asDict() for row in conflicts.collect()]}")
    window = Window.partitionBy(*group_by)
    return df.withColumn(fill_column, F.coalesce(F.col(fill_column), F.max(fill_column).over(window)))
