import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # Spark's Python workers must run this interpreter, not whatever `python` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()
