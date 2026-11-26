from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType


def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    # No `if field.name in df.columns`: a missing Silver column must fail here, not land as nulls (#135)
    schema_fields = [F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields]

    return df.select(schema_fields)
