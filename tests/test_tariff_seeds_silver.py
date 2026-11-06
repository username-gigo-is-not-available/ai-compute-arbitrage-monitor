"""Silver tariff seeds handle every tariff version in Bronze, independent of row order."""

import pytest
from fakes import RecordedWrite, ViewNames
from pyspark.sql import SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from lakehouse.partition import Partition
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.electricity_tariff_tiers import ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA
from refine.seeds.electricity_tariff_tiers import ElectricityTariffTiersPipeline
from refine.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowSchedulePipeline

OLD = "цените ќе се применуваат почнувајќи од 01.01.2025."
NEW = "цените ќе се применуваат почнувајќи од 01.01.2026."


def _tiers(session: SparkSession) -> ElectricityTariffTiersPipeline:
    return ElectricityTariffTiersPipeline(
        session=session,
        dataset=Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS),
        config=None,
        lakehouse_config=None,
        silver_table=SilverTable(ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA, OverwriteByPartition()),
    )


def _schedule(session: SparkSession) -> ElectricityTariffWindowSchedulePipeline:
    return ElectricityTariffWindowSchedulePipeline(
        session=session,
        dataset=Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE, dataset_type=DatasetType.SEEDS),
        config=None,
        lakehouse_config=None,
        silver_table=SilverTable(ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA, OverwriteByPartition()),
    )


def test_tier_labels_fill_from_their_own_version_whatever_the_row_order(spark):
    # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
    # The labelled row comes last within each version, so a forward fill would leave the others empty.
    bronze = spark.sql(
        "SELECT consumer_category, label, metric, value, tariff_tier, valid_from_text, "
        "CAST('2026-10-12 00:00:00' AS TIMESTAMP) AS ingested_at FROM VALUES "
        f"('household', '', '', '1,0', 'ВТ 1', '{OLD}'), "
        f"('household', 'Old label', 'kWh', '2,0', 'ВТ 2', '{OLD}'), "
        f"('household', '', '', '3,0', 'ВТ 1', '{NEW}'), "
        f"('household', 'New label', 'MWh', '4,0', 'ВТ 2', '{NEW}') "
        "AS t(consumer_category, label, metric, value, tariff_tier, valid_from_text)"
    )
    rows = {(str(r["valid_from"]), r["label"], r["metric"]) for r in _tiers(spark).transform(bronze).collect()}
    assert rows == {("2025-01-01", "Old label", "kWh"), ("2026-01-01", "New label", "MWh")}


def test_each_version_comes_from_its_latest_fetch(spark):
    # Bronze appends every scrape. NEW was fetched on two days, OLD only on the first, after which EVN dropped it.
    bronze = spark.sql(
        "SELECT consumer_category, label, metric, value, tariff_tier, valid_from_text, "
        "CAST(fetched AS TIMESTAMP) AS ingested_at FROM VALUES "
        f"('household', 'Old label', 'kWh', '1,0', 'ВТ1', '{OLD}', '2026-10-12 00:00:00'), "
        f"('household', 'New label', 'kWh', '3,0', 'ВТ1', '{NEW}', '2026-10-12 00:00:00'), "
        f"('household', 'New label', 'kWh', '4,0', 'ВТ2', '{NEW}', '2026-10-12 00:00:00'), "
        f"('household', 'New label', 'kWh', '3,5', 'ВТ1', '{NEW}', '2026-10-13 00:00:00'), "
        f"('household', 'New label', 'kWh', '4,5', 'ВТ2', '{NEW}', '2026-10-13 00:00:00') "
        "AS t(consumer_category, label, metric, value, tariff_tier, valid_from_text, fetched)"
    )
    rows = {
        (str(r["valid_from"]), r["tariff_tier"], float(r["value"])) for r in _tiers(spark).transform(bronze).collect()
    }
    assert rows == {("2025-01-01", "BT1", 1.0), ("2026-01-01", "BT1", 3.5), ("2026-01-01", "BT2", 4.5)}


def test_schedule_fetched_twice_gives_one_grid(spark):
    text = "Ниската тарифа е од 22:00 до 07:00 часот секој ден, како и викенд."
    bronze = spark.sql(
        "SELECT schedule_text, valid_from_text, CAST(fetched AS TIMESTAMP) AS ingested_at FROM VALUES "
        f"('{text}', '{NEW}', '2026-10-12 00:00:00'), "
        f"('{text}', '{NEW}', '2026-10-13 00:00:00') "
        "AS t(schedule_text, valid_from_text, fetched)"
    )
    rows = _schedule(spark).generate(bronze).collect()
    assert len(rows) == 7 * 24
    assert {str(r["ingested_at"]) for r in rows} == {"2026-10-13 00:00:00"}


def test_two_labels_in_one_version_fail_loudly(spark):
    bronze = spark.sql(
        "SELECT consumer_category, label, metric, value, tariff_tier, valid_from_text, "
        "CAST('2026-10-12 00:00:00' AS TIMESTAMP) AS ingested_at FROM VALUES "
        f"('household', 'Label A', 'kWh', '1,0', 'ВТ 1', '{NEW}'), "
        f"('household', 'Label B', '', '2,0', 'ВТ 2', '{NEW}') "
        "AS t(consumer_category, label, metric, value, tariff_tier, valid_from_text)"
    )
    with pytest.raises(ValueError, match="label has more than one value"):
        _tiers(spark).transform(bronze)


def test_schedule_generates_one_week_grid_per_version(spark):
    bronze = spark.sql(
        "SELECT schedule_text, valid_from_text, CAST('2026-10-12 00:00:00' AS TIMESTAMP) AS ingested_at "
        "FROM VALUES "
        f"('Ниската тарифа е од 22:00 до 07:00 часот секој ден, како и викенд.', '{OLD}'), "
        f"('Ниската тарифа е од 13:00 до 15:00 часот секој ден, како и викенд.', '{NEW}') "
        "AS t(schedule_text, valid_from_text)"
    )
    rows = _schedule(spark).generate(bronze).collect()
    assert len(rows) == 2 * 7 * 24
    monday_low = {
        (str(r["valid_from"]), r["start_hour"])
        for r in rows
        if r["day_of_week"] == 1 and r["tariff_window_type"] == "low"
    }
    assert monday_low == {("2025-01-01", h) for h in [*range(22, 24), *range(7)]} | {
        ("2026-01-01", h) for h in range(13, 15)
    }


def test_run_with_no_unseen_fetches_writes_nothing(spark):
    # Most days Silver has seen every fetch (ADR-023), so the read is empty and there is no version to grid.
    pipeline = _schedule(spark)
    empty = spark.sql(
        f"SELECT 'x' AS schedule_text, '{OLD}' AS valid_from_text, CAST(NULL AS TIMESTAMP) AS ingested_at"
    ).filter("false")
    saved = []
    pipeline.read = lambda: empty
    pipeline.save = saved.append
    pipeline.run()
    assert saved == []


def test_run_writes_the_generated_grid_to_silver(spark):
    # An empty Silver has seen no fetch, so the whole Bronze schedule is read and becomes one week of hours.
    spark.sql("SELECT CAST(NULL AS TIMESTAMP) AS ingested_at").filter("false").createOrReplaceTempView(
        "silver_electricity_tariff_window_schedule"
    )
    spark.sql(
        "SELECT schedule_text, valid_from_text, CAST('2026-10-12 00:00:00' AS TIMESTAMP) AS ingested_at FROM VALUES "
        f"('Ниската тарифа е од 22:00 до 07:00 часот секој ден, како и викенд.', '{NEW}') "
        "AS t(schedule_text, valid_from_text)"
    ).createOrReplaceTempView("bronze_electricity_tariff_window_schedule")
    load = RecordedWrite(Partition("valid_from"))
    pipeline = ElectricityTariffWindowSchedulePipeline(
        session=spark,
        dataset=Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE, dataset_type=DatasetType.SEEDS),
        config=None,
        lakehouse_config=ViewNames(),
        silver_table=SilverTable(ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA, load),
    )

    pipeline.run()

    rows = load.written["silver_electricity_tariff_window_schedule"]
    assert len(rows) == 7 * 24
    assert {str(r["valid_from"]) for r in rows} == {"2026-01-01"}
