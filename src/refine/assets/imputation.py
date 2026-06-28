from pyspark.sql import DataFrame, Window, WindowSpec
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def forward_fill(df: DataFrame, order_by_column: str, fill_column: str) -> DataFrame:
    window: WindowSpec = Window.orderBy(col(order_by_column)).rowsBetween(Window.unboundedPreceding, 0)

    return df.withColumn(
        fill_column,
        F.last(col(fill_column), ignorenulls=True).over(window)
    )
