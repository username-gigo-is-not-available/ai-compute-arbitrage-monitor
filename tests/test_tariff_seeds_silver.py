"""Silver tariff seeds handle every tariff version in Bronze, independent of row order.

Run directly:   uv run python tests/test_tariff_seeds_silver.py
Under pytest:   uv run --extra dev pytest tests/test_tariff_seeds_silver.py
"""

from __future__ import annotations

import os
import sys
import unittest

from pyspark.sql import SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.electricity_tariff_tiers import ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA
from refine.seeds.electricity_tariff_tiers import ElectricityTariffTiersPipeline
from refine.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowSchedulePipeline

OLD = "цените ќе се применуваат почнувајќи од 01.01.2025."
NEW = "цените ќе се применуваат почнувајќи од 01.01.2026."


class TestTariffSeedsSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    def _pipeline(self, cls, name, schema):
        return cls(session=self.session,
                   dataset=Dataset(dataset_name=name, dataset_type=DatasetType.SEEDS),
                   config=None, lakehouse_config=None,
                   silver_table=SilverTable(schema, OverwriteByPartition()))

    def test_tier_labels_fill_from_their_own_version_whatever_the_row_order(self):
        # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
        # The labelled row comes last within each version, so a forward fill would leave the others empty.
        bronze = self.session.sql(
            "SELECT consumer_category, label, metric, value, tariff_tier, valid_from_text, "
            "CAST('2026-10-12 00:00:00' AS TIMESTAMP) AS ingested_at FROM VALUES "
            f"('household', '', '', '1,0', 'ВТ 1', '{OLD}'), "
            f"('household', 'Old label', 'kWh', '2,0', 'ВТ 2', '{OLD}'), "
            f"('household', '', '', '3,0', 'ВТ 1', '{NEW}'), "
            f"('household', 'New label', 'MWh', '4,0', 'ВТ 2', '{NEW}') "
            "AS t(consumer_category, label, metric, value, tariff_tier, valid_from_text)"
        )
        pipeline = self._pipeline(ElectricityTariffTiersPipeline, DatasetName.ELECTRICITY_TARIFF_TIERS,
                                  ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA)
        rows = {(str(r["valid_from"]), r["label"], r["metric"]) for r in pipeline.transform(bronze).collect()}
        self.assertEqual(rows, {("2025-01-01", "Old label", "kWh"), ("2026-01-01", "New label", "MWh")})

    def test_two_labels_in_one_version_fail_loudly(self):
        bronze = self.session.sql(
            "SELECT consumer_category, label, metric, value, tariff_tier, valid_from_text, "
            "CAST('2026-10-12 00:00:00' AS TIMESTAMP) AS ingested_at FROM VALUES "
            f"('household', 'Label A', 'kWh', '1,0', 'ВТ 1', '{NEW}'), "
            f"('household', 'Label B', '', '2,0', 'ВТ 2', '{NEW}') "
            "AS t(consumer_category, label, metric, value, tariff_tier, valid_from_text)"
        )
        pipeline = self._pipeline(ElectricityTariffTiersPipeline, DatasetName.ELECTRICITY_TARIFF_TIERS,
                                  ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA)
        with self.assertRaisesRegex(ValueError, "label has more than one value"):
            pipeline.transform(bronze)

    def test_schedule_generates_one_week_grid_per_version(self):
        bronze = self.session.sql(
            "SELECT schedule_text, valid_from_text, CAST('2026-10-12 00:00:00' AS TIMESTAMP) AS ingested_at "
            "FROM VALUES "
            f"('Ниската тарифа е од 22:00 до 07:00 часот секој ден, како и викенд.', '{OLD}'), "
            f"('Ниската тарифа е од 13:00 до 15:00 часот секој ден, како и викенд.', '{NEW}') "
            "AS t(schedule_text, valid_from_text)"
        )
        pipeline = self._pipeline(ElectricityTariffWindowSchedulePipeline,
                                  DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
                                  ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA)
        rows = pipeline.generate(bronze).collect()
        self.assertEqual(len(rows), 2 * 7 * 24)
        monday_low = {(str(r["valid_from"]), r["start_hour"]) for r in rows
                      if r["day_of_week"] == 1 and r["tariff_window_type"] == "low"}
        self.assertEqual(monday_low, {("2025-01-01", h) for h in [*range(22, 24), *range(0, 7)]}
                         | {("2026-01-01", h) for h in range(13, 15)})


if __name__ == "__main__":
    unittest.main()
