"""ComputeOffersIngestor fetches the on-demand market as a census of ask_contract_id ranges (ADR-020).

The ingestor's HttpClient sends through a fake market instead of requests.get; no network, no quota.
"""

import json
import random
from collections.abc import Callable
from datetime import UTC, datetime
from unittest import mock

import pytest
import requests
from fakes import FakeResponse
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from common.classes import Dataset, Snapshot
from common.enums import DatasetName, DatasetType
from config.apis.vast_ai import VastAIConfig
from config.http import HttpConfig
from config.lakehouse import GCPLakehouseConfig
from config.loader import ConfigLoader
from ingest.http_client import HttpClient
from ingest.schemas.compute_offers import COMPUTE_OFFERS_BRONZE_SCHEMA
from ingest.sources.compute_offers import ComputeOffersIngestor
from ingest.write_strategy import AppendByDay, BronzeTable

OFFER = {
    "ask_contract_id": 8_936_325,
    "machine_id": 15_489,
    "host_id": 333,
    "dph_base": 0.80,
    "discounted_dph_total": 0.8017,
    "min_bid": 0.7786,
    "dlperf_per_dphtotal": 12.0,
    "gpu_arch": "nvidia",
    "gpu_name": "RTX 4090",
    "gpu_ram": 24_564,
    "cpu_ram": 96_000,
    "num_gpus": 2,
    "gpu_frac": 0.4,
    "gpu_ids": [55_783, 55_784],
    "disk_space": 512,
    "inet_down": 1000,
    "inet_up": 100,
    "verification": "verified",
    "rentable": True,
    "rented": False,
}


def _offers(offers: list[dict]) -> FakeResponse:
    return FakeResponse(json_data={"offers": offers})


class FakeMarket:
    """Behaves like /bundles: a range matching more offers than the limit returns a random sample of them."""

    def __init__(self, offer_ids: list[int], seed: int = 0) -> None:
        self.offers = [{**OFFER, "ask_contract_id": i} for i in offer_ids]
        self.random = random.Random(seed)

    def get(self, url: str, params: dict, **kwargs) -> FakeResponse:
        q = json.loads(params["q"])
        lo, hi = q["ask_contract_id"]["gte"], q["ask_contract_id"].get("lt")
        matches = [o for o in self.offers if o["ask_contract_id"] >= lo and (hi is None or o["ask_contract_id"] < hi)]
        limit = q["limit"]
        return _offers(self.random.sample(matches, limit) if len(matches) > limit else matches)


def _ingestor(send: Callable[..., FakeResponse]) -> ComputeOffersIngestor:
    return ComputeOffersIngestor(
        dataset=Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES),
        config=VastAIConfig(
            enabled=True,
            base_url="https://console.vast.test/api/v0",
            limit=512,
            split_at=500,
            census_parts=32,
            request_interval_seconds=0,
        ),
        lakehouse_config=GCPLakehouseConfig(catalog_id="test", project_id="test", warehouse="gs://test"),
        bronze_table=BronzeTable(
            Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False)),
            AppendByDay(column="snapshot_at"),
        ),
        http=HttpClient(HttpConfig(timeout_seconds=30, retry_count=1, retry_delay_seconds=0), send=send),
        snapshot=Snapshot(at=datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)),
    )


def _queries(get_mock: mock.Mock) -> list[dict]:
    return [json.loads(call.kwargs["params"]["q"]) for call in get_mock.call_args_list]


def test_requests_on_demand_offers_by_ask_contract_id_range():
    get_mock = mock.Mock(side_effect=[_offers([OFFER])])
    offers = _ingestor(get_mock).load()
    assert _queries(get_mock) == [{"type": "on-demand", "limit": 512, "ask_contract_id": {"gte": 0}}]
    assert [o.offer_id for o in offers] == [8_936_325]


def test_market_over_the_limit_returns_every_offer_exactly_once():
    ids = random.Random(1).sample(range(7_000_000, 54_000_000), 3_000)
    offers = _ingestor(FakeMarket(ids).get).load()
    returned = [o.offer_id for o in offers]
    assert len(returned) == 3_000
    assert set(returned) == set(ids)


def test_a_range_that_comes_back_full_fails_the_run():
    # 3,000 offers in 4 ranges is ~750 per range: over the limit, so a range would only be a sample
    ids = random.Random(1).sample(range(7_000_000, 54_000_000), 3_000)
    ingestor = _ingestor(FakeMarket(ids).get)
    ingestor.config = ingestor.config.model_copy(update={"census_parts": 4})
    with pytest.raises(RuntimeError, match="raise census_parts"):
        ingestor.load()


def test_configured_census_costs_about_the_market_size():
    # The daily quota counts returned rows, and a full range's rows are thrown away when it is split; the
    # configured split must keep that overhead small (a 4-way split cost ~2x on 2026-10-03 and ran out of quota).
    configured = ConfigLoader().get_vast_ai()
    ids = random.Random(3).sample(range(7_000_000, 54_000_000), 10_000)
    ingestor = _ingestor(FakeMarket(ids).get)
    ingestor.config = ingestor.config.model_copy(update={"census_parts": configured.census_parts})
    offers = ingestor.load()
    assert len(offers) == 10_000
    assert ingestor.rows_used <= 11_500


def test_bronze_rows_keep_the_slice_of_the_machine():
    # gpu_frac gives machine size (num_gpus / gpu_frac); gpu_ids shows which slices overlap (ADR-020).
    row = _ingestor(mock.Mock(side_effect=[_offers([OFFER])])).load()[0].to_row()
    assert row["gpu_fraction_of_machine"] == 0.4
    assert row["gpu_ids"] == [55_783, 55_784]
    assert {"gpu_fraction_of_machine", "gpu_ids"} <= set(COMPUTE_OFFERS_BRONZE_SCHEMA.column_names)


def test_a_failed_range_writes_nothing_and_fails_the_run():
    # The first range comes back full, so the census splits it; the second request is quota-exhausted.
    full_page = [{**OFFER, "ask_contract_id": i} for i in range(8_000_000, 8_000_512)]
    ingestor = _ingestor(mock.Mock(side_effect=[_offers(full_page), FakeResponse(status=429)]))
    with mock.patch.object(ingestor, "store") as store_mock, pytest.raises(requests.HTTPError) as raised:
        ingestor.run()
    store_mock.assert_not_called()
    assert raised.value.response.status_code == 429
