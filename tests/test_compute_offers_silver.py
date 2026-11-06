"""Silver idempotency for compute_offers on snapshot_at (ADR-019)."""

from pyiceberg.transforms import DayTransform
from pyspark.sql import DataFrame, SparkSession

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from lakehouse.partition import Partition
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.compute_offers import COMPUTE_OFFERS_SILVER_SCHEMA
from refine.sources.compute_offers import ComputeOffersPipeline, deduplicate_compute_offers

OVERWRITE_BY_DAY = OverwriteByPartition(Partition("snapshot_at", DayTransform()))


def _offers(session: SparkSession, rows: str) -> DataFrame:
    # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
    return session.sql(
        "SELECT offer_id, offer_type, CAST(price AS DOUBLE) AS price, CAST(snap AS TIMESTAMP) AS snapshot_at, "
        "CAST(fetched AS TIMESTAMP) AS ingested_at "
        f"FROM VALUES {rows} AS t(offer_id, offer_type, price, snap, fetched)"
    )


def test_watermark_reads_every_fetch_silver_has_not_seen(spark):
    # Silver holds the 14:00 snapshot fetched at 14:00:05. A re-run of the same snapshot at 14:40 is a newer
    # fetch, so it is read and overwrites that day, as a rebuild from Bronze would.
    _offers(spark, "(1, 'bid', 0.10, '2026-10-01 14:00:00', '2026-10-01 14:00:05')").createOrReplaceTempView(
        "silver_offers"
    )
    _offers(
        spark,
        "(1, 'bid', 0.10, '2026-10-01 14:00:00', '2026-10-01 14:00:05'), "
        "(1, 'bid', 0.11, '2026-10-01 14:00:00', '2026-10-01 14:40:00'), "
        "(1, 'bid', 0.12, '2026-10-01 15:00:00', '2026-10-01 15:00:04')",
    ).createOrReplaceTempView("bronze_offers")
    result = OVERWRITE_BY_DAY.read(spark, "bronze_offers", "silver_offers")
    assert sorted(r["price"] for r in result.collect()) == [0.11, 0.12]


def test_empty_silver_reads_all_of_bronze(spark):
    _offers(spark, "(1, 'bid', 0.10, '2026-10-01 14:00:00', '2026-10-01 14:00:05')").filter(
        "false"
    ).createOrReplaceTempView("empty_silver_offers")
    _offers(
        spark,
        "(1, 'bid', 0.10, '2026-10-01 14:00:00', '2026-10-01 14:00:05'), "
        "(1, 'bid', 0.12, '2026-10-01 15:00:00', '2026-10-01 15:00:04')",
    ).createOrReplaceTempView("bronze_offers")
    result = OVERWRITE_BY_DAY.read(spark, "bronze_offers", "empty_silver_offers")
    assert sorted(r["price"] for r in result.collect()) == [0.10, 0.12]


def test_retry_replaces_whole_earlier_attempt_not_merges(spark):
    # Attempt 1 saw offers 1 and 2; the retry saw offers 1 and 3. The snapshot is the retry's view only.
    batch = _offers(
        spark,
        "(1, 'bid', 0.11, '2026-10-01 15:00:00', '2026-10-01 15:00:04'), "
        "(2, 'bid', 0.20, '2026-10-01 15:00:00', '2026-10-01 15:00:04'), "
        "(1, 'bid', 0.12, '2026-10-01 15:00:00', '2026-10-01 15:05:09'), "
        "(3, 'bid', 0.30, '2026-10-01 15:00:00', '2026-10-01 15:05:09'), "
        "(4, 'bid', 0.40, '2026-10-01 16:00:00', '2026-10-01 16:00:03')",
    )
    rows = {(r["offer_id"], r["price"]) for r in deduplicate_compute_offers(batch).collect()}
    assert rows == {(1, 0.12), (3, 0.30), (4, 0.40)}


def test_silver_keeps_the_slice_of_the_machine(spark):
    # dbt derives machine size as number_of_offer_gpus / gpu_fraction_of_machine (ADR-020).
    bronze = spark.sql(
        "SELECT 8936325 AS offer_id, 'on_demand' AS offer_type, 'AMD EPYC 7B13' AS cpu_model_name, "
        "2 AS number_of_offer_gpus, CAST(0.4 AS DOUBLE) AS gpu_fraction_of_machine, "
        "array(CAST(55783 AS BIGINT), CAST(55784 AS BIGINT)) AS gpu_ids, "
        "CAST('2026-10-03 12:00:00' AS TIMESTAMP) AS snapshot_at, "
        "CAST('2026-10-03 12:00:04' AS TIMESTAMP) AS ingested_at"
    )
    pipeline = ComputeOffersPipeline(
        session=spark,
        dataset=Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES),
        config=None,
        lakehouse_config=None,
        silver_table=SilverTable(COMPUTE_OFFERS_SILVER_SCHEMA, OVERWRITE_BY_DAY),
    )
    row = pipeline.transform(bronze).collect()[0]
    assert row["number_of_offer_gpus"] == 2
    assert row["gpu_fraction_of_machine"] == 0.4
    assert row["gpu_ids"] == [55783, 55784]
