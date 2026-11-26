import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

from refine.assets.casting import cast_to_schema

SCHEMA = StructType([StructField("offer_id", LongType()), StructField("price", DoubleType())])


def test_casts_every_schema_column(spark):
    df = spark.range(1).select(F.lit("7").alias("offer_id"), F.lit("0.3").alias("price"), F.lit(1).alias("extra"))

    out = cast_to_schema(df, SCHEMA)

    assert out.schema == SCHEMA
    assert out.first().asDict() == {"offer_id": 7, "price": 0.3}


def test_missing_schema_column_fails(spark):
    df = spark.range(1).select(F.lit("7").alias("offer_id"))

    with pytest.raises(AnalysisException, match="price"):
        cast_to_schema(df, SCHEMA)
