"""Retry-behavior tests for the HTTP-calling ingestors (EVN, exchange rates, Vast.ai).

The three ingestors wire tenacity's ``@retry`` decorator (driven by the shared
``HttpConfig.retry_count`` / ``retry_delay_seconds``) around their raw HTTP GETs.
These tests mock at the HTTP seam — no network, no third-party test deps, no real
sleeps (``time.sleep`` / ``asyncio.sleep`` are patched and asserted instead):

  * transient failures (non-200 statuses OR transport errors) followed by a 200
    must produce a successful parse — not an empty list — for all three ingestors;
  * a permanent non-200 failure logs the pre-existing ERROR text *exactly once*
    and returns []/None as before, after ``retry_count`` attempts and
    ``retry_count - 1`` sleeps of ``retry_delay_seconds`` each;
  * a permanent transport error re-raises after exhaustion (so Airflow still
    observes a failed task), matching the pre-change uncaught behavior;
  * ``retry_count=1`` is the degenerate case: one attempt, zero sleeps — identical
    to the old single-attempt behavior.

Run directly (no deps added, no network touched):

    uv run python tests/test_http_retry.py

Run under pytest (optional, once the dev-extra is installed):

    uv run --extra dev pytest tests/test_http_retry.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import traceback
from http import HTTPStatus
from unittest import mock

import aiohttp
import requests

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from config.apis.evn import EVNConfig
from config.apis.exchange_rate import ExchangeRateConfig
from config.apis.vast_ai import VastAIConfig
from config.http import HttpConfig
from config.storage import GCPStorageConfig
from ingest.evn_base import EVNBaseIngestor
from ingest.sources.compute_offers import ComputeOffersIngestor
from ingest.sources.exchange_rates import ExchangeRateIngestor

HTML = "<html><body><div><h1>tariffs</h1></div></body></html>"

EXCHANGE_JSON = {
    "conversion_rates": {"MKD": 58.15},
    "time_last_update_utc": "Tue, 03 Sep 2026 00:00:00 +0000",
}

OFFER_JSON = {
    "offers": [
        {
            "ask_contract_id": 111,
            "machine_id": 222,
            "host_id": 333,
            "dph_base": 0.50,
            "discounted_dph_total": 0.45,
            "dlperf_per_dphtotal": 12.0,
            "gpu_arch": "NVIDIA",
            "gpu_name": "RTX 4090",
            "gpu_ram": 24,
            "cpu_ram": 96,
            "disk_space": 512,
            "inet_down": 1000,
            "inet_up": 100,
            "verification": "verified",
            "rentable": True,
            "rented": False,
        }
    ]
}

HTTP_RETRY_COUNT = 3
HTTP_RETRY_DELAY_SECONDS = 60


def _http(retry_count: int = HTTP_RETRY_COUNT, retry_delay_seconds: int = HTTP_RETRY_DELAY_SECONDS) -> HttpConfig:
    return HttpConfig(
        timeout_seconds=30,
        retry_count=retry_count,
        retry_delay_seconds=retry_delay_seconds,
    )


def _storage() -> GCPStorageConfig:
    return GCPStorageConfig(bucket_name="test-bucket")


class FakeResponse:
    """Stands in for both requests.Response and aiohttp.ClientResponse at the status level used here."""

    def __init__(self, status: int = HTTPStatus.OK, text: str = "", json_data: dict | None = None):
        self.status = status
        self.status_code = status
        self.text = text
        self._json_data = json_data if json_data is not None else {}
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def release(self) -> None:
        self.closed = True

    async def json(self, **kwargs) -> dict:
        return self._json_data


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _capture_logs(ingestor) -> _Capture:
    logger = logging.getLogger(ingestor.name)
    handler = _Capture()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return handler


def _errors(handler: _Capture) -> list[logging.LogRecord]:
    return [r for r in handler.records if r.levelno == logging.ERROR]


def _warnings(handler: _Capture) -> list[logging.LogRecord]:
    return [r for r in handler.records if r.levelno == logging.WARNING]


def _sleep_calls(sleep_mock) -> list:
    return sleep_mock.call_args_list


# ---------------------------------------------------------------------------
# EVN (sync): EVNBaseIngestor.fetch_soup  ->  requests.get + time.sleep
# ---------------------------------------------------------------------------

class _EVNTestIngestor(EVNBaseIngestor):
    def load(self):
        return []

    def parse(self, **kwargs):
        return None


def _evn(retry_count: int) -> _EVNTestIngestor:
    return _EVNTestIngestor(
        dataset=Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS),
        config=EVNConfig(enabled=True, tariff_tiers_url="https://evn.test/", tariff_system_url="https://evn.test/"),
        storage_config=_storage(),
        http_config=_http(retry_count=retry_count),
    )


def test_evn_transient_500_then_200_parses_soup() -> None:
    ingestor = _evn(retry_count=3)
    handler = _capture_logs(ingestor)
    with mock.patch.object(requests, "get", side_effect=[FakeResponse(500), FakeResponse(500), FakeResponse(200, text=HTML)]) as get_mock, \
            mock.patch("time.sleep") as sleep_mock:
        soup = ingestor.fetch_soup("https://evn.test/")

    assert soup is not None, "transient 500s should retry and parse the 200 response"
    assert soup.select_one("h1").text == "tariffs"
    assert get_mock.call_count == 3
    assert _sleep_calls(sleep_mock) == [mock.call(60), mock.call(60)]
    assert _errors(handler) == []
    assert len(_warnings(handler)) == 2


def test_evn_permanent_500_returns_none_and_logs_error_once() -> None:
    ingestor = _evn(retry_count=3)
    handler = _capture_logs(ingestor)
    with mock.patch.object(requests, "get", side_effect=[FakeResponse(500), FakeResponse(500), FakeResponse(500)]) as get_mock, \
            mock.patch("time.sleep") as sleep_mock:
        result = ingestor.fetch_soup("https://evn.test/")

    assert result is None
    assert get_mock.call_count == 3
    assert _sleep_calls(sleep_mock) == [mock.call(60), mock.call(60)]
    errors = _errors(handler)
    assert len(errors) == 1
    assert errors[0].getMessage() == "EVN tariff fetch returned HTTP status: 500."


def test_evn_transport_exception_then_200_parses_soup() -> None:
    ingestor = _evn(retry_count=2)
    handler = _capture_logs(ingestor)
    with mock.patch.object(
        requests,
        "get",
        side_effect=[requests.exceptions.ConnectionError("connection refused"), FakeResponse(200, text=HTML)],
    ) as get_mock, mock.patch("time.sleep") as sleep_mock:
        soup = ingestor.fetch_soup("https://evn.test/")

    assert soup is not None
    assert get_mock.call_count == 2
    assert _sleep_calls(sleep_mock) == [mock.call(60)]
    assert _errors(handler) == []
    assert len(_warnings(handler)) == 1


def test_evn_permanent_transport_exception_reraises() -> None:
    ingestor = _evn(retry_count=2)
    handler = _capture_logs(ingestor)
    with mock.patch.object(
        requests,
        "get",
        side_effect=[requests.exceptions.ConnectionError("connection refused"), requests.exceptions.ReadTimeout("read timeout")],
    ) as get_mock, mock.patch("time.sleep") as sleep_mock:
        try:
            ingestor.fetch_soup("https://evn.test/")
        except requests.exceptions.RequestException as exc:
            passed = isinstance(exc, requests.exceptions.ReadTimeout)
        else:
            passed = False

    assert passed, "permanent transport error must re-raise after exhaustion"
    assert get_mock.call_count == 2
    assert _sleep_calls(sleep_mock) == [mock.call(60)]
    assert _errors(handler) == []


def test_evn_retry_count_1_single_attempt_no_sleep() -> None:
    ingestor = _evn(retry_count=1)
    handler = _capture_logs(ingestor)
    with mock.patch.object(requests, "get", side_effect=[FakeResponse(500)]) as get_mock, \
            mock.patch("time.sleep") as sleep_mock:
        result = ingestor.fetch_soup("https://evn.test/")

    assert result is None
    assert get_mock.call_count == 1
    assert _sleep_calls(sleep_mock) == []
    assert len(_errors(handler)) == 1


# ---------------------------------------------------------------------------
# Exchange rates (async): ExchangeRateIngestor.load -> aiohttp + asyncio.sleep
# ---------------------------------------------------------------------------

def _exchange(retry_count: int) -> ExchangeRateIngestor:
    return ExchangeRateIngestor(
        dataset=Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES),
        config=ExchangeRateConfig(
            enabled=True,
            base_url="https://exchangerate.test/",
            from_currency="USD",
            to_currency="MKD",
            api_key="test-key",
        ),
        storage_config=_storage(),
        http_config=_http(retry_count=retry_count),
    )


def test_exchange_transient_500_then_200_parses() -> None:
    ingestor = _exchange(retry_count=3)
    handler = _capture_logs(ingestor)
    side_effects = [FakeResponse(500), FakeResponse(500), FakeResponse(200, json_data=EXCHANGE_JSON)]
    with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=side_effects)) as get_mock, \
            mock.patch("asyncio.sleep", new=mock.AsyncMock()) as sleep_mock:
        result = asyncio.run(ingestor.load())

    assert len(result) == 1, "transient 500s should retry and parse the 200 response"
    assert result[0].value == 58.15
    assert get_mock.await_count == 3
    assert _sleep_calls(sleep_mock) == [mock.call(60), mock.call(60)]
    assert _errors(handler) == []
    assert len(_warnings(handler)) == 2


def test_exchange_permanent_500_returns_empty_and_logs_error_once() -> None:
    ingestor = _exchange(retry_count=3)
    handler = _capture_logs(ingestor)
    side_effects = [FakeResponse(500), FakeResponse(500), FakeResponse(500)]
    with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=side_effects)) as get_mock, \
            mock.patch("asyncio.sleep", new=mock.AsyncMock()) as sleep_mock:
        result = asyncio.run(ingestor.load())

    assert result == []
    assert get_mock.await_count == 3
    assert _sleep_calls(sleep_mock) == [mock.call(60), mock.call(60)]
    errors = _errors(handler)
    assert len(errors) == 1
    assert errors[0].getMessage() == "Exchange Rate API poll returned HTTP status: 500."


def test_exchange_timeout_transport_error_retried_then_success() -> None:
    ingestor = _exchange(retry_count=2)
    handler = _capture_logs(ingestor)
    side_effects = [asyncio.TimeoutError("total timeout"), FakeResponse(200, json_data=EXCHANGE_JSON)]
    with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=side_effects)) as get_mock, \
            mock.patch("asyncio.sleep", new=mock.AsyncMock()) as sleep_mock:
        result = asyncio.run(ingestor.load())

    assert len(result) == 1
    assert get_mock.await_count == 2
    assert _sleep_calls(sleep_mock) == [mock.call(60)]
    assert _errors(handler) == []


def test_exchange_retry_count_1_single_attempt_no_sleep() -> None:
    ingestor = _exchange(retry_count=1)
    handler = _capture_logs(ingestor)
    with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=[FakeResponse(500)])) as get_mock, \
            mock.patch("asyncio.sleep", new=mock.AsyncMock()) as sleep_mock:
        result = asyncio.run(ingestor.load())

    assert result == []
    assert get_mock.await_count == 1
    assert _sleep_calls(sleep_mock) == []
    assert len(_errors(handler)) == 1


# ---------------------------------------------------------------------------
# Vast.ai compute offers (async): ComputeOffersIngestor.load, per offer_type
# ---------------------------------------------------------------------------

def _compute(retry_count: int) -> ComputeOffersIngestor:
    return ComputeOffersIngestor(
        dataset=Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES),
        config=VastAIConfig(enabled=True, base_url="https://console.vast.test/api/v0", limit=10),
        storage_config=_storage(),
        http_config=_http(retry_count=retry_count),
    )


def test_compute_transient_500_then_200_parses_all_offer_types() -> None:
    ingestor = _compute(retry_count=3)
    handler = _capture_logs(ingestor)
    offer_ok = FakeResponse(200, json_data=OFFER_JSON)
    side_effects = [
        FakeResponse(500),
        FakeResponse(500),
        offer_ok,
        offer_ok,
        offer_ok,
    ]
    with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=side_effects)) as get_mock, \
            mock.patch("asyncio.sleep", new=mock.AsyncMock()) as sleep_mock:
        result = asyncio.run(ingestor.load())

    assert len(result) == 3, "first offer_type needed retries, then all three parses succeeded"
    assert get_mock.await_count == 5
    assert _sleep_calls(sleep_mock) == [mock.call(60), mock.call(60)]
    assert _errors(handler) == []
    assert len(_warnings(handler)) == 2


def test_compute_permanent_500_returns_empty_and_logs_error_once() -> None:
    ingestor = _compute(retry_count=3)
    handler = _capture_logs(ingestor)
    side_effects = [FakeResponse(500), FakeResponse(500), FakeResponse(500)]
    with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=side_effects)) as get_mock, \
            mock.patch("asyncio.sleep", new=mock.AsyncMock()) as sleep_mock:
        result = asyncio.run(ingestor.load())

    assert result == []
    assert get_mock.await_count == 3
    assert _sleep_calls(sleep_mock) == [mock.call(60), mock.call(60)]
    errors = _errors(handler)
    assert len(errors) == 1
    assert errors[0].getMessage() == "Vast.AI API returned HTTP 500"


def test_compute_retry_count_1_single_attempt_no_sleep() -> None:
    ingestor = _compute(retry_count=1)
    handler = _capture_logs(ingestor)
    with mock.patch.object(aiohttp.ClientSession, "get", new=mock.AsyncMock(side_effect=[FakeResponse(500)])) as get_mock, \
            mock.patch("asyncio.sleep", new=mock.AsyncMock()) as sleep_mock:
        result = asyncio.run(ingestor.load())

    assert result == []
    assert get_mock.await_count == 1
    assert _sleep_calls(sleep_mock) == []
    assert len(_errors(handler)) == 1


# ---------------------------------------------------------------------------
# Standalone runner (also collectable by pytest: the test_* functions above)
# ---------------------------------------------------------------------------

_TESTS = [
    test_evn_transient_500_then_200_parses_soup,
    test_evn_permanent_500_returns_none_and_logs_error_once,
    test_evn_transport_exception_then_200_parses_soup,
    test_evn_permanent_transport_exception_reraises,
    test_evn_retry_count_1_single_attempt_no_sleep,
    test_exchange_transient_500_then_200_parses,
    test_exchange_permanent_500_returns_empty_and_logs_error_once,
    test_exchange_timeout_transport_error_retried_then_success,
    test_exchange_retry_count_1_single_attempt_no_sleep,
    test_compute_transient_500_then_200_parses_all_offer_types,
    test_compute_permanent_500_returns_empty_and_logs_error_once,
    test_compute_retry_count_1_single_attempt_no_sleep,
]


def _run_standalone() -> int:
    # The tests print "❯" arrows and such, which the Windows cp1252 console
    # cannot always encode; force UTF-8 output so the assertions are what run.
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass
    failures = 0
    for t in _TESTS:
        try:
            t()
        except Exception:  # noqa: BLE001 - report all failures in the standalone run
            failures += 1
            print(f"FAIL  {t.__name__}")
            traceback.print_exc()
        else:
            print(f"PASS  {t.__name__}")
    total = len(_TESTS)
    print(f"\n{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(_run_standalone())