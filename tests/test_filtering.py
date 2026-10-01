"""Unit tests for refine.assets.filtering.

Run directly:   uv run python tests/test_filtering.py
Under pytest:   uv run --extra dev pytest tests/test_filtering.py
"""

from __future__ import annotations

import os
import sys
import unittest

from pyspark.sql import SparkSession

from refine.assets.filtering import deduplicate_keep_latest


class TestDeduplicateKeepLatest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    def test_keeps_row_with_latest_order_column_per_key(self):
        # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
        df = self.session.sql(
            "SELECT k, v, CAST(at AS TIMESTAMP) AS ingested_at FROM VALUES "
            "('a', 1.0, '2026-09-30 10:00:00'), "
            "('a', 2.0, '2026-09-30 12:00:00'), "
            "('a', 3.0, '2026-09-30 11:00:00'), "
            "('b', 9.0, '2026-09-30 10:00:00') "
            "AS t(k, v, at)"
        )
        result = deduplicate_keep_latest(df, columns=["k"], order_by="ingested_at")
        rows = {r["k"]: r["v"] for r in result.collect()}
        self.assertEqual(rows, {"a": 2.0, "b": 9.0})
        self.assertEqual(result.columns, df.columns)


if __name__ == "__main__":
    unittest.main()
