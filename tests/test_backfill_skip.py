"""Every ingestor skips a past day without fetching: its source serves only its current state (ADR-019, #61)."""

from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest
from fakes import build_ingestor

from common.classes import Snapshot
from common.enums import DatasetName, DatasetType
from config.apis.exchange_rate import ExchangeRateConfig
from config.apis.vast_ai import VastAIConfig
from config.loader import ConfigLoader
from ingest.seeds.electricity_tariff_blocks import ElectricityTariffBlocksIngestor
from ingest.seeds.electricity_tariff_fees import ElectricityTariffFeesIngestor
from ingest.seeds.electricity_tariff_tiers import ElectricityTariffTiersIngestor
from ingest.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowScheduleIngestor
from ingest.sources.compute_offers import ComputeOffersIngestor
from ingest.sources.exchange_rates import ExchangeRateIngestor

VAST_AI = VastAIConfig(
    enabled=True,
    base_url="https://console.vast.test/api/v0",
    limit=512,
    split_at=500,
    census_parts=4,
    request_interval_seconds=0,
)
EXCHANGE_RATE = ExchangeRateConfig(
    enabled=True, base_url="https://exchangerate.test/", from_currency="USD", to_currency="MKD", api_key="test-key"
)
EVN = ConfigLoader().get_evn()

INGESTORS = [
    (ComputeOffersIngestor, DatasetName.COMPUTE_OFFERS, DatasetType.SOURCES, VAST_AI),
    (ExchangeRateIngestor, DatasetName.EXCHANGE_RATES, DatasetType.SOURCES, EXCHANGE_RATE),
    (ElectricityTariffTiersIngestor, DatasetName.ELECTRICITY_TARIFF_TIERS, DatasetType.SEEDS, EVN),
    (ElectricityTariffFeesIngestor, DatasetName.ELECTRICITY_TARIFF_FEES, DatasetType.SEEDS, EVN),
    (ElectricityTariffBlocksIngestor, DatasetName.ELECTRICITY_TARIFF_BLOCKS, DatasetType.SEEDS, EVN),
    (
        ElectricityTariffWindowScheduleIngestor,
        DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
        DatasetType.SEEDS,
        EVN,
    ),
]
TODAY = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _ingestor(ingestor_class, name, dataset_type, config, snapshot_at: datetime):
    return build_ingestor(ingestor_class, name, dataset_type, config, snapshot=Snapshot(at=snapshot_at))


@pytest.mark.parametrize(
    ("ingestor_class", "name", "dataset_type", "config"), INGESTORS, ids=lambda v: getattr(v, "value", None)
)
def test_a_past_day_is_skipped_without_fetching(ingestor_class, name, dataset_type, config):
    # build_ingestor's default send fails the test on any request, so returning quietly means nothing was fetched
    _ingestor(ingestor_class, name, dataset_type, config, TODAY - timedelta(hours=25)).run()


@pytest.mark.parametrize(
    ("ingestor_class", "name", "dataset_type", "config"), INGESTORS, ids=lambda v: getattr(v, "value", None)
)
def test_today_is_fetched(ingestor_class, name, dataset_type, config):
    ingestor = _ingestor(ingestor_class, name, dataset_type, config, TODAY)
    with mock.patch.object(ingestor, "load", return_value=[]) as load, mock.patch.object(ingestor, "handle_result"):
        ingestor.run()
    load.assert_called_once()
