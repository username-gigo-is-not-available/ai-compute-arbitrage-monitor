"""Silver for exchange_rates: one rate per currency pair and timestamp, latest ingest wins."""

import pytest
from fakes import RecordedWrite, ViewNames
from pyspark.sql import DataFrame, SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from lakehouse.partition import Partition
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.exchange_rates import EXCHANGE_RATES_SILVER_SCHEMA
from refine.sources.exchange_rates import ExchangeRatesPipeline


def _rates(session: SparkSession, rows: str) -> DataFrame:
    # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
    return session.sql(
        "SELECT from_currency, to_currency, CAST(value AS DOUBLE) AS value, CAST(ts AS TIMESTAMP) AS timestamp, "
        "CAST(fetched AS TIMESTAMP) AS ingested_at "
        f"FROM VALUES {rows} AS t(from_currency, to_currency, value, ts, fetched)"
    )


@pytest.fixture
def pipeline(spark) -> ExchangeRatesPipeline:
    return ExchangeRatesPipeline(
        session=spark,
        dataset=Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES),
        config=None,
        lakehouse_config=None,
        silver_table=SilverTable(EXCHANGE_RATES_SILVER_SCHEMA, OverwriteByPartition(Partition("timestamp"))),
    )


def test_retry_keeps_the_latest_ingest_of_each_rate(spark, pipeline):
    # The provider publishes one rate a day, so a retry fetches the same value again; ingested_at picks the
    # later fetch. Another day's rate and another pair are separate rows.
    bronze = _rates(
        spark,
        "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05'), "
        "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:30:00'), "
        "('EUR', 'USD', 1.07, '2026-10-13 00:00:00', '2026-10-13 00:00:04'), "
        "('MKD', 'USD', 0.0175, '2026-10-12 00:00:00', '2026-10-12 00:00:05')",
    )
    rows = {
        (r["from_currency"], str(r["timestamp"].date()), r["value"], str(r["ingested_at"]))
        for r in pipeline.transform(bronze).collect()
    }
    assert rows == {
        ("EUR", "2026-10-12", 1.08, "2026-10-12 00:30:00"),
        ("EUR", "2026-10-13", 1.07, "2026-10-13 00:00:04"),
        ("MKD", "2026-10-12", 0.0175, "2026-10-12 00:00:05"),
    }


def test_currency_codes_are_trimmed_and_empty_becomes_null(spark, pipeline):
    # Silver does not drop the empty code: the not_null dbt test is what catches it.
    bronze = _rates(
        spark,
        "(' EUR ', 'USD ', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05'), "
        "('', 'USD', 0.0175, '2026-10-12 00:00:00', '2026-10-12 00:00:05')",
    )
    rows = {(r["from_currency"], r["to_currency"]) for r in pipeline.transform(bronze).collect()}
    assert rows == {("EUR", "USD"), (None, "USD")}


def test_output_has_the_silver_columns_in_order(spark, pipeline):
    result = pipeline.transform(_rates(spark, "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05')"))
    assert result.columns == ["from_currency", "to_currency", "value", "timestamp", "ingested_at", "processed_at"]
    assert result.collect()[0]["processed_at"] is not None


def test_run_writes_the_unseen_fetches_to_silver(spark):
    # Silver has seen the 00:00:05 fetch; the 00:30 retry and the next day's fetch are new.
    _rates(spark, "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05')").createOrReplaceTempView(
        "silver_exchange_rates"
    )
    _rates(
        spark,
        "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:00:05'), "
        "('EUR', 'USD', 1.08, '2026-10-12 00:00:00', '2026-10-12 00:30:00'), "
        "('EUR', 'USD', 1.07, '2026-10-13 00:00:00', '2026-10-13 00:00:04')",
    ).createOrReplaceTempView("bronze_exchange_rates")
    load = RecordedWrite(Partition("timestamp"))
    pipeline = ExchangeRatesPipeline(
        session=spark,
        dataset=Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES),
        config=None,
        lakehouse_config=ViewNames(),
        silver_table=SilverTable(EXCHANGE_RATES_SILVER_SCHEMA, load),
    )

    pipeline.run()

    assert list(load.written) == ["silver_exchange_rates"]
    assert {(str(r["timestamp"].date()), str(r["ingested_at"])) for r in load.written["silver_exchange_rates"]} == {
        ("2026-10-12", "2026-10-12 00:30:00"),
        ("2026-10-13", "2026-10-13 00:00:04"),
    }
