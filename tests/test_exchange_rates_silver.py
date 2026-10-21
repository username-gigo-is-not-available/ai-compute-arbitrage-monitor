"""Silver for exchange_rates: one rate per currency pair and timestamp, latest ingest wins.

Run directly:   uv run python tests/test_exchange_rates_silver.py
Under pytest:   uv run --extra dev pytest tests/test_exchange_rates_silver.py
"""

from __future__ import annotations

import os
import sys
import unittest

from pyspark.sql import SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.exchange_rates import EXCHANGE_RATES_SILVER_SCHEMA
from refine.sources.exchange_rates import ExchangeRatesPipeline


def _rates(session: SparkSession, rows: str):
    # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
    return session.sql(
        "SELECT from_currency, to_currency, CAST(value AS DOUBLE) AS value, CAST(ts AS TIMESTAMP) AS timestamp, "
        "CAST(fetched AS TIMESTAMP) AS ingested_at "
        f"FROM VALUES {rows} AS t(from_currency, to_currency, value, ts, fetched)"
    )


class TestExchangeRatesSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        cls.pipeline = ExchangeRatesPipeline(
            session=cls.session,
            dataset=Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES),
            config=None,
            lakehouse_config=None,
            silver_table=SilverTable(EXCHANGE_RATES_SILVER_SCHEMA, OverwriteByPartition(column="timestamp")),
        )

    def test_retry_keeps_the_latest_ingest_of_each_rate(self):
        # The provider publishes one rate a day, so a retry fetches the same value again; ingested_at picks the
        # later fetch. Another day's rate and another pair are separate rows.
        bronze = _rates(self.session,
                        "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05'), "
                        "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:30:00'), "
                        "('EUR', 'USD', 1.07, '2026-10-13 00:00:00', '2026-10-13 00:00:04'), "
                        "('MKD', 'USD', 0.0175, '2026-10-12 00:00:00', '2026-10-12 00:00:05')")
        rows = {(r["from_currency"], str(r["timestamp"].date()), r["value"], str(r["ingested_at"]))
                for r in self.pipeline.transform(bronze).collect()}
        self.assertEqual(rows, {("EUR", "2026-10-12", 1.08, "2026-10-12 00:30:00"),
                                ("EUR", "2026-10-13", 1.07, "2026-10-13 00:00:04"),
                                ("MKD", "2026-10-12", 0.0175, "2026-10-12 00:00:05")})

    def test_currency_codes_are_trimmed_and_empty_becomes_null(self):
        # Silver does not drop the empty code: the not_null dbt test is what catches it.
        bronze = _rates(self.session,
                        "(' EUR ', 'USD ', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05'), "
                        "('', 'USD', 0.0175, '2026-10-12 00:00:00', '2026-10-12 00:00:05')")
        rows = {(r["from_currency"], r["to_currency"]) for r in self.pipeline.transform(bronze).collect()}
        self.assertEqual(rows, {("EUR", "USD"), (None, "USD")})

    def test_output_has_the_silver_columns_in_order(self):
        bronze = _rates(self.session, "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05')")
        result = self.pipeline.transform(bronze)
        self.assertEqual(result.columns,
                         ["from_currency", "to_currency", "value", "timestamp", "ingested_at", "processed_at"])
        self.assertIsNotNone(result.collect()[0]["processed_at"])


if __name__ == "__main__":
    unittest.main()
