"""ComputeOffersIngestor stamps offers with the scheduled snapshot_at and refuses backfills (ADR-019).

Run directly:   uv run python tests/test_compute_offers_snapshot.py
Under pytest:   uv run --extra dev pytest tests/test_compute_offers_snapshot.py
"""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, UTC
from unittest import mock

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from common.classes import Dataset
from common.enums import DatasetName, DatasetType, OfferType
from config.apis.vast_ai import VastAIConfig
from config.http import HttpConfig
from config.lakehouse import GCPLakehouseConfig
from ingest.sources.compute_offers import ComputeOffersIngestor
from ingest.write_strategy import AppendByHour

OFFER = {
    "ask_contract_id": 111, "machine_id": 222, "host_id": 333,
    "dph_base": 0.50, "discounted_dph_total": 0.45, "dlperf_per_dphtotal": 12.0,
    "gpu_arch": "NVIDIA", "gpu_name": "RTX 4090", "gpu_ram": 24, "cpu_ram": 96,
    "disk_space": 512, "inet_down": 1000, "inet_up": 100,
    "verification": "verified", "rentable": True, "rented": False,
}


def _ingestor(snapshot_at: datetime) -> ComputeOffersIngestor:
    return ComputeOffersIngestor(
        dataset=Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES),
        config=VastAIConfig(enabled=True, base_url="https://console.vast.test/api/v0", limit=10),
        lakehouse_config=GCPLakehouseConfig(catalog_id="test", project_id="test", warehouse="gs://test"),
        bronze_schema=Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False)),
        write_strategy=AppendByHour(column="snapshot_at"),
        http_config=HttpConfig(timeout_seconds=30, retry_count=1, retry_delay_seconds=0),
        snapshot_at=snapshot_at,
    )


class TestSnapshotAt(unittest.TestCase):

    def test_parse_stamps_scheduled_snapshot_at_and_real_ingested_at(self):
        snapshot_at = datetime(2026, 10, 1, 14, tzinfo=UTC)
        fetched = datetime(2026, 10, 1, 14, 0, 7, tzinfo=UTC)
        offer = _ingestor(snapshot_at).parse(data=OFFER, timestamp=fetched, offer_type=OfferType.ON_DEMAND)
        self.assertEqual(offer.snapshot_at, snapshot_at)
        self.assertEqual(offer.ingested_at, fetched)
        self.assertEqual(offer.to_row()["snapshot_at"], snapshot_at)

    def test_backfill_is_skipped_without_touching_the_catalog(self):
        ingestor = _ingestor(datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=3))
        with mock.patch.object(ingestor, "init") as init_mock, mock.patch.object(ingestor, "load") as load_mock:
            asyncio.run(ingestor.run())
        init_mock.assert_not_called()
        load_mock.assert_not_called()

    def test_current_hour_runs(self):
        ingestor = _ingestor(datetime.now(UTC).replace(minute=0, second=0, microsecond=0))

        async def no_offers():
            return []

        with mock.patch.object(ingestor, "init") as init_mock, mock.patch.object(ingestor, "load", side_effect=no_offers):
            asyncio.run(ingestor.run())
        init_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
