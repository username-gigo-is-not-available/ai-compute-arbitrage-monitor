"""HttpClient retries every ingestor's GET; each rule is tested once, here, at the client's interface.

No network and no patching: the client is given a fake `send` (in place of requests.get) and a fake `sleep`.

Run under pytest:   uv run --extra dev pytest tests/test_http_client.py
"""

from __future__ import annotations

import logging
from unittest import mock

import pytest
import requests
from fakes import FakeResponse
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from common.classes import Dataset, Snapshot
from common.enums import DatasetName, DatasetType
from config.apis.evn import EVNConfig
from config.apis.exchange_rate import ExchangeRateConfig
from config.http import HttpConfig
from config.lakehouse import GCPLakehouseConfig
from ingest.evn_base import EVNBaseIngestor
from ingest.http_client import HttpClient
from ingest.sources.exchange_rates import ExchangeRateIngestor
from ingest.write_strategy import AppendByDay, BronzeTable

RETRY_DELAY_SECONDS = 60


class FakeServer:
    """Answers each GET with the next outcome: a response, or an exception to raise. Records calls and sleeps."""

    def __init__(self, *outcomes: FakeResponse | Exception) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict]] = []
        self.sleeps: list[float] = []

    def send(self, url: str, **kwargs) -> FakeResponse:
        self.calls.append((url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def client(self, retry_count: int = 3) -> HttpClient:
        config = HttpConfig(timeout_seconds=30, retry_count=retry_count, retry_delay_seconds=RETRY_DELAY_SECONDS)
        return HttpClient(config, send=self.send, sleep=self.sleeps.append)


# ---------------------------------------------------------------------------
# Retry rules
# ---------------------------------------------------------------------------


def test_a_200_is_returned_without_retrying_and_passes_the_timeout_and_arguments():
    server = FakeServer(FakeResponse(text="ok"))
    response = server.client().get("https://x.test/", params={"q": "1"})

    assert response.text == "ok"
    assert server.calls == [("https://x.test/", {"timeout": 30, "params": {"q": "1"}})]
    assert server.sleeps == []


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_transient_500s_are_retried_until_a_200():
    server = FakeServer(FakeResponse(500), FakeResponse(500), FakeResponse(text="ok"))
    logger, handler = logging.getLogger("ingest.http_client"), _Capture()
    logger.addHandler(handler)
    try:
        response = server.client(retry_count=3).get("https://x.test/")
    finally:
        logger.removeHandler(handler)

    assert response.text == "ok"
    assert len(server.calls) == 3
    assert server.sleeps == [60, 60]
    assert len(handler.records) == 2, "one warning per retry"


def test_a_failed_response_is_closed_before_retrying():
    failed = FakeResponse(500)
    FakeServer(failed, FakeResponse()).client().get("https://x.test/")
    assert failed.closed


def test_a_permanent_500_raises_after_retry_count_attempts():
    server = FakeServer(FakeResponse(500), FakeResponse(500), FakeResponse(500))
    with pytest.raises(requests.HTTPError) as raised:
        server.client(retry_count=3).get("https://x.test/")

    assert raised.value.response.status_code == 500
    assert len(server.calls) == 3
    assert server.sleeps == [60, 60]


def test_a_transport_error_is_retried():
    server = FakeServer(requests.exceptions.ConnectionError("connection refused"), FakeResponse(text="ok"))
    assert server.client(retry_count=2).get("https://x.test/").text == "ok"
    assert server.sleeps == [60]


def test_a_permanent_transport_error_reraises_the_last_one():
    server = FakeServer(
        requests.exceptions.ConnectionError("connection refused"), requests.exceptions.ReadTimeout("read timeout")
    )
    with pytest.raises(requests.exceptions.ReadTimeout):
        server.client(retry_count=2).get("https://x.test/")
    assert len(server.calls) == 2


def test_retry_count_1_is_a_single_attempt_without_sleeping():
    server = FakeServer(FakeResponse(500))
    with pytest.raises(requests.HTTPError):
        server.client(retry_count=1).get("https://x.test/")
    assert len(server.calls) == 1
    assert server.sleeps == []


def test_a_short_retry_after_is_waited_out():
    # Longer than retry_delay_seconds but within the budget (3 x 60 s): wait what the server asks, then retry
    server = FakeServer(FakeResponse(429, headers={"Retry-After": "90"}), FakeResponse(text="ok"))
    assert server.client(retry_count=3).get("https://x.test/").text == "ok"
    assert server.sleeps == [90]


def test_a_retry_after_beyond_the_budget_fails_without_retrying():
    # Vast.ai's daily row quota answers 429 with a Retry-After of hours; waiting cannot help
    server = FakeServer(FakeResponse(429, headers={"Retry-After": "30129"}))
    with pytest.raises(requests.HTTPError) as raised:
        server.client(retry_count=5).get("https://x.test/")

    assert raised.value.response.status_code == 429
    assert len(server.calls) == 1
    assert server.sleeps == []


def test_a_retry_after_shorter_than_the_delay_waits_the_delay():
    server = FakeServer(FakeResponse(503, headers={"Retry-After": "5"}), FakeResponse())
    server.client().get("https://x.test/")
    assert server.sleeps == [RETRY_DELAY_SECONDS]


@pytest.mark.parametrize("headers", [{}, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}])
def test_a_retry_after_missing_or_not_in_seconds_waits_the_delay(headers):
    server = FakeServer(FakeResponse(503, headers=headers), FakeResponse())
    server.client().get("https://x.test/")
    assert server.sleeps == [RETRY_DELAY_SECONDS]


# ---------------------------------------------------------------------------
# What the ingestors still own: reading the response and refusing an empty fetch
# ---------------------------------------------------------------------------

_DUMMY_BRONZE_TABLE = BronzeTable(
    Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False)),
    AppendByDay(column="ingested_at"),
)
_LAKEHOUSE = GCPLakehouseConfig(catalog_id="test", project_id="test", warehouse="gs://test")


class _EVNTestIngestor(EVNBaseIngestor):
    def load(self):
        return []


def _evn(client: HttpClient) -> _EVNTestIngestor:
    return _EVNTestIngestor(
        dataset=Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS),
        config=EVNConfig(enabled=True, tariff_tiers_url="https://evn.test/", tariff_system_url="https://evn.test/"),
        lakehouse_config=_LAKEHOUSE,
        bronze_table=_DUMMY_BRONZE_TABLE,
        http=client,
        snapshot=Snapshot.from_scheduled_at(None),
    )


def test_evn_parses_the_page_the_client_returns():
    client = FakeServer(FakeResponse(text="<html><body><h1>tariffs</h1></body></html>")).client()
    assert _evn(client).fetch_soup("https://evn.test/").select_one("h1").text == "tariffs"


def test_exchange_rates_parse_the_json_the_client_returns():
    client = FakeServer(
        FakeResponse(
            json_data={
                "conversion_rates": {"MKD": 58.15},
                "time_last_update_utc": "Tue, 03 Sep 2026 00:00:00 +0000",
            }
        )
    ).client()
    ingestor = ExchangeRateIngestor(
        dataset=Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES),
        config=ExchangeRateConfig(
            enabled=True,
            base_url="https://exchangerate.test/",
            from_currency="USD",
            to_currency="MKD",
            api_key="test-key",
        ),
        lakehouse_config=_LAKEHOUSE,
        bronze_table=_DUMMY_BRONZE_TABLE,
        http=client,
        snapshot=Snapshot.from_scheduled_at(None),
    )
    assert [r.value for r in ingestor.load()] == [58.15]


def test_an_empty_fetch_fails_the_run_instead_of_storing_nothing():
    ingestor = _evn(FakeServer().client())
    with mock.patch.object(ingestor, "store") as store_mock, pytest.raises(RuntimeError, match="fetched no records"):
        ingestor.handle_result([])
    store_mock.assert_not_called()
