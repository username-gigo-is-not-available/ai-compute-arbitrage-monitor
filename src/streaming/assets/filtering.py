from pyspark.sql import functions as F, Column
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType


def filter_integration_test_rows(df: DataFrame) -> DataFrame:
    return df.filter(
        ~F.col("open_db_id").rlike("^open-db-(create|update|delete)")
    )


def filter_null(df: DataFrame, col: str) -> DataFrame:
    return df.filter(F.col(col).isNotNull())