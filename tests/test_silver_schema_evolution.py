"""An existing Silver table gains the columns a refined batch adds, where the batch puts them (ADR-020).

Real Iceberg in local Spark: a Hadoop catalog in a temp directory, using the iceberg-spark-runtime the project
already resolves (cached by Ivy after the first local refine run).

Run directly:   uv run python tests/test_silver_schema_evolution.py
Under pytest:   uv run --extra dev pytest tests/test_silver_schema_evolution.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from refine.write_strategy import AppendByHour

CATALOG = "test_catalog"
TABLE = f"{CATALOG}.silver_sources.compute_offers"


class TestSilverSchemaEvolution(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.tmp = tempfile.TemporaryDirectory()
        cls.session = (
            SparkSession.builder.master("local[1]").appName("test")
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")
            .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", Path(cls.tmp.name).as_uri())
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.session.stop()
        cls.tmp.cleanup()

    def _batch(self, columns: str):
        return self.session.sql(f"SELECT {columns}")

    def test_existing_table_gains_new_columns_in_batch_order(self):
        strategy = AppendByHour(column="snapshot_at")
        strategy.write(self._batch(
            "1 AS offer_id, 4 AS number_of_offer_gpus, 12.1 AS gpu_max_cuda_version_supported, "
            "CAST('2026-10-03 11:00:00' AS TIMESTAMP) AS snapshot_at"
        ), TABLE)
        strategy.write(self._batch(
            "2 AS offer_id, 2 AS number_of_offer_gpus, CAST(0.4 AS DOUBLE) AS gpu_fraction_of_machine, "
            "array(CAST(55783 AS BIGINT), CAST(55784 AS BIGINT)) AS gpu_ids, 12.1 AS gpu_max_cuda_version_supported, "
            "CAST('2026-10-03 12:00:00' AS TIMESTAMP) AS snapshot_at"
        ), TABLE)
        table = self.session.table(TABLE)
        self.assertEqual(table.columns, ["offer_id", "number_of_offer_gpus", "gpu_fraction_of_machine", "gpu_ids",
                                         "gpu_max_cuda_version_supported", "snapshot_at"])
        rows = {r["offer_id"]: (r["gpu_fraction_of_machine"], r["gpu_ids"]) for r in table.collect()}
        self.assertEqual(rows, {1: (None, None), 2: (0.4, [55783, 55784])})


if __name__ == "__main__":
    unittest.main()
