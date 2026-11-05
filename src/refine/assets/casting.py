from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType


def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    schema_fields = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in schema.fields
        if field.name in df.columns
    ]

    return df.select(schema_fields)
