"""ComputeOffersIngestor fetches the on-demand market as a census of ask_contract_id ranges (ADR-020).

Mocks at the HTTP seam (aiohttp.ClientSession.get); no network, no quota.

Run directly:   uv run python tests/test_compute_offers_census.py
Under pytest:   uv run --extra dev pytest tests/test_compute_offers_census.py
"""

from __future__ import annotations

import asyncio
import json
import random
import unittest
from datetime import datetime, UTC
from unittest import mock

import aiohttp
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from config.apis.vast_ai import VastAIConfig
from config.http import HttpConfig
from config.lakehouse import GCPLakehouseConfig
from config.loader import ConfigLoader
from ingest.schemas.compute_offers import COMPUTE_OFFERS_BRONZE_SCHEMA
from ingest.sources.compute_offers import CensusIncompleteError, ComputeOffersIngestor
from ingest.write_strategy import AppendByHour, BronzeTable

OFFER = {
    "ask_contract_id": 8_936_325, "machine_id": 15_489, "host_id": 333,
    "dph_base": 0.80, "discounted_dph_total": 0.8017, "min_bid": 0.7786, "dlperf_per_dphtotal": 12.0,
    "gpu_arch": "nvidia", "gpu_name": "RTX 4090", "gpu_ram": 24_564, "cpu_ram": 96_000,
    "num_gpus": 2, "gpu_frac": 0.4, "gpu_ids": [55_783, 55_784],
    "disk_space": 512, "inet_down": 1000, "inet_up": 100,
    "verification": "verified", "rentable": True, "rented": False,
}


class FakeResponse:
    def __init__(self, status: int = 200, offers: list[dict] | None = None) -> None:
        self.status = status
        self.headers = {}
        self.offers = offers or []

    def close(self) -> None:
        pass

    async def json(self, **kwargs) -> dict:
        return {"offers": self.offers}


class FakeMarket:
    """Behaves like /bundles: a range matching more offers than the limit returns a random sample of them."""

    def __init__(self, offer_ids: list[int], seed: int = 0) -> None:
        self.offers = [{**OFFER, "ask_contract_id": i} for i in offer_ids]
        self.random = random.Random(seed)

    async def get(self, url: str, params: dict, **kwargs) -> FakeResponse:
        q = json.loads(params["q"])
        lo, hi = q["ask_contract_id"]["gte"], q["ask_contract_id"].get("lt")
        matches = [o for o in self.offers if o["ask_contract_id"] >= lo and (hi is None or o["ask_contract_id"] < hi)]
        limit = q["limit"]
        return FakeResponse(offers=self.random.sample(matches, limit) if len(matches) > limit else matches)


def _ingestor() -> ComputeOffersIngestor:
    return ComputeOffersIngestor(
        dataset=Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES),
        config=VastAIConfig(enabled=True, base_url="https://console.vast.test/api/v0", limit=512, split_at=500,
                            census_parts=4, request_interval_seconds=0),
        lakehouse_config=GCPLakehouseConfig(catalog_id="test", project_id="test", warehouse="gs://test"),
        bronze_table=BronzeTable(
            Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False)),
            AppendByHour(column="snapshot_at"),
        ),
        http_config=HttpConfig(timeout_seconds=30, retry_count=1, retry_delay_seconds=0),
        snapshot_at=datetime.now(UTC).replace(minute=0, second=0, microsecond=0),
    )


def _queries(get_mock: mock.AsyncMock) -> list[dict]:
    return [json.loads(call.kwargs["params"]["q"]) for call in get_mock.await_args_list]


class TestComputeOffersCensus(unittest.TestCase):

    def test_requests_on_demand_offers_by_ask_contract_id_range(self):
        get_mock = mock.AsyncMock(side_effect=[FakeResponse(offers=[OFFER])])
        with mock.patch.object(aiohttp.ClientSession, "get", new=get_mock):
            offers = asyncio.run(_ingestor().load())
        self.assertEqual(_queries(get_mock), [{"type": "on-demand", "limit": 512, "ask_contract_id": {"gte": 0}}])
        self.assertEqual([o.offer_id for o in offers], [8_936_325])

    def test_market_over_the_limit_returns_every_offer_exactly_once(self):
        ids = random.Random(1).sample(range(7_000_000, 54_000_000), 3_000)
        with mock.patch.object(aiohttp.ClientSession, "get", new=FakeMarket(ids).get):
            offers = asyncio.run(_ingestor().load())
        returned = [o.offer_id for o in offers]
        self.assertEqual(len(returned), 3_000)
        self.assertEqual(set(returned), set(ids))

    def test_configured_census_costs_about_the_market_size(self):
        # The daily quota counts returned rows, and a full range's rows are thrown away when it is split; the
        # configured split must keep that overhead small (a 4-way split cost ~2x on 2026-10-03 and ran out of quota).
        configured = ConfigLoader().get_vast_ai()
        ingestor = _ingestor()
        ingestor.config = ingestor.config.model_copy(update={"census_parts": configured.census_parts})
        ids = random.Random(3).sample(range(7_000_000, 54_000_000), 10_000)
        with mock.patch.object(aiohttp.ClientSession, "get", new=FakeMarket(ids).get):
            offers = asyncio.run(ingestor.load())
        self.assertEqual(len(offers), 10_000)
        self.assertLessEqual(ingestor.rows_used, 11_500)

    def test_bronze_rows_keep_the_slice_of_the_machine(self):
        # gpu_frac gives machine size (num_gpus / gpu_frac); gpu_ids shows which slices overlap (ADR-020).
        with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=[FakeResponse(offers=[OFFER])])):
            row = asyncio.run(_ingestor().load())[0].to_row()
        self.assertEqual(row["gpu_fraction_of_machine"], 0.4)
        self.assertEqual(row["gpu_ids"], [55_783, 55_784])
        self.assertTrue({"gpu_fraction_of_machine", "gpu_ids"} <= set(COMPUTE_OFFERS_BRONZE_SCHEMA.column_names))

    def test_a_failed_range_writes_nothing_and_fails_the_run(self):
        # The first range comes back full, so the census splits it; the second request is quota-exhausted.
        full_page = [{**OFFER, "ask_contract_id": i} for i in range(8_000_000, 8_000_512)]
        ingestor = _ingestor()
        get_mock = mock.AsyncMock(side_effect=[FakeResponse(offers=full_page), FakeResponse(status=429)])
        with mock.patch.object(aiohttp.ClientSession, "get", new=get_mock), \
                mock.patch.object(ingestor, "ensure_bronze_table"), \
                mock.patch.object(ingestor, "store") as store_mock, \
                self.assertRaises(CensusIncompleteError):
            asyncio.run(ingestor.run())
        store_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
