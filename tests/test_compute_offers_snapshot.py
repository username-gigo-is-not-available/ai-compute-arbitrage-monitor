"""ComputeOffersIngestor stamps offers with the scheduled snapshot_at (ADR-019)."""

from datetime import UTC, datetime

from fakes import build_ingestor

from common.classes import Snapshot
from common.enums import DatasetName, DatasetType, OfferType
from config.apis.vast_ai import VastAIConfig
from ingest.sources.compute_offers import ComputeOffersIngestor

OFFER = {
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


VAST_AI = VastAIConfig(
    enabled=True,
    base_url="https://console.vast.test/api/v0",
    limit=512,
    split_at=500,
    census_parts=4,
    request_interval_seconds=0,
    api_key="test-key",
)


def _ingestor(snapshot_at: datetime) -> ComputeOffersIngestor:
    return build_ingestor(
        ComputeOffersIngestor,
        DatasetName.COMPUTE_OFFERS,
        DatasetType.SOURCES,
        VAST_AI,
        snapshot=Snapshot(at=snapshot_at),
    )


def test_parse_stamps_scheduled_snapshot_at_and_real_ingested_at():
    snapshot_at = datetime(2026, 10, 1, 14, tzinfo=UTC)
    fetched = datetime(2026, 10, 1, 14, 0, 7, tzinfo=UTC)
    offer = _ingestor(snapshot_at).parse(data=OFFER, ingested_at=fetched, offer_type=OfferType.ON_DEMAND)
    assert offer.snapshot_at == snapshot_at
    assert offer.ingested_at == fetched
    assert offer.to_row()["snapshot_at"] == snapshot_at
