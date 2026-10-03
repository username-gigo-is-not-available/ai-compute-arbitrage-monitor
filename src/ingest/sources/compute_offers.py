import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, UTC
from http import HTTPStatus
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout
from pydantic import ValidationError

from common.classes import Dataset
from common.enums import OfferType, DatasetType, DatasetName
from config.apis.vast_ai import VastAIConfig
from config.loader import ConfigLoader
from ingest.base import AsyncIngestor
from ingest.models.vast_ai_offer import VastAIOffer
from ingest.schemas.compute_offers import COMPUTE_OFFERS_BRONZE_SCHEMA
from ingest.scheduling import is_backfill, resolve_snapshot_at
from ingest.write_strategy import AppendByHour, BronzeTable


class CensusIncompleteError(Exception):
    """A range of the market could not be fetched, so the census is not the whole market (ADR-020)."""


@dataclass
class ComputeOffersIngestor(AsyncIngestor):
    snapshot_at: datetime
    # Vast.ai's daily quota counts returned rows, including those of ranges that had to be split.
    rows_used: int = field(init=False, default=0)
    requests: int = field(init=False, default=0)

    async def run(self) -> None:
        if is_backfill(self.snapshot_at):
            self.logger.warning(
                f"Skipping {self.name}: snapshot_at {self.snapshot_at.isoformat()} is in the past and "
                f"Vast.ai only serves the current market, so it cannot be backfilled (ADR-019)."
            )
            return
        await super().run()

    async def load(self) -> list[VastAIOffer]:
        # On-demand only: its rows carry the bid price (min_bid), and reserved prices equal on-demand (ADR-020).
        async with ClientSession() as session:
            ingested_at: datetime = datetime.now(UTC)
            rows = await self.cover(session, 0, None)
            self.logger.info(f"Census of {len(rows)} offers used {self.rows_used} rows in {self.requests} requests")
            offers = [self.parse(data=row, timestamp=ingested_at, offer_type=OfferType.ON_DEMAND) for row in rows]
            return [offer for offer in offers if offer]

    async def cover(self, session: ClientSession, lo: int, hi: int | None) -> list[dict[str, Any]]:
        # /bundles returns a random sample when more offers match than the limit (ADR-020). A range returning fewer
        # than split_at is complete; a fuller one is split at the sample's quantiles, which keeps every part
        # non-empty and narrower than the range.
        offers = await self.search(session, lo, hi)
        if len(offers) < self.config.split_at:
            return offers
        ids = sorted(o["ask_contract_id"] for o in offers)
        parts = self.config.census_parts
        cuts = sorted({ids[len(ids) * i // parts] for i in range(1, parts)})
        bounds = [lo, *cuts, hi]
        covered = []
        for part_lo, part_hi in zip(bounds, bounds[1:]):
            covered += await self.cover(session, part_lo, part_hi)
        return covered

    async def search(self, session: ClientSession, lo: int, hi: int | None) -> list[dict[str, Any]]:
        response = await self.fetch_async(
            (ClientError, asyncio.TimeoutError),
            session.get,
            self.config.url,
            headers=self.config.header,
            params=self.config.params(lo, hi),
            timeout=ClientTimeout(total=self.http_config.timeout_seconds),
        )
        if response.status != HTTPStatus.OK:
            self.logger.error(f"Vast.AI API returned HTTP {response.status}")
            raise CensusIncompleteError(f"range [{lo}, {hi}) failed with HTTP {response.status}")
        try:
            data: dict[str, Any] = await response.json(encoding="utf-8")
        finally:
            response.close()
        await asyncio.sleep(self.config.request_interval_seconds)
        offers = data.get("offers", [])
        self.requests += 1
        self.rows_used += len(offers)
        self.logger.info(f"Range [{lo}, {hi}): {len(offers)} offers; {self.rows_used} rows used in {self.requests} requests")
        return offers

    def parse(self, **kwargs) -> VastAIOffer | None:
        data: dict[str, Any] = kwargs.get("data")
        ingested_at: datetime = kwargs.get("timestamp")
        offer_type: OfferType = kwargs.get("offer_type")
        try:
            return VastAIOffer(
                snapshot_at=self.snapshot_at,
                ingested_at=ingested_at,
                offer_id=data.get("ask_contract_id"),
                machine_id=data.get("machine_id"),
                host_id=data.get("host_id"),
                offer_type=offer_type,
                gpu_price_usd_per_hr=data.get("dph_base"),
                total_price_usd_per_hr=data.get("discounted_dph_total"),
                minimum_bid_price_usd=data.get("min_bid", 0),
                storage_cost_usd_per_hr=data.get("storage_total_cost"),
                network_upload_cost_usd_per_gbit=data.get("inet_up_cost"),
                network_download_cost_usd_per_gbit=data.get("inet_down_cost"),
                deep_learning_score_per_usd=data.get("dlperf_per_dphtotal"),
                gpu_architecture=data.get("gpu_arch"),
                gpu_model_name=data.get("gpu_name"),
                gpu_memory_mb=data.get("gpu_ram"),
                gpu_tdp_watts=data.get("gpu_max_power"),
                number_of_gpus=data.get("num_gpus", 1),
                gpu_fraction_of_machine=data.get("gpu_frac"),
                gpu_ids=data.get("gpu_ids"),
                gpu_max_cuda_version_supported=data.get("cuda_max_good"),
                gpu_tflops=data.get("total_flops"),
                gpu_bandwidth_gbytes_per_sec=data.get("gpu_mem_bw"),
                cpu_architecture=data.get("cpu_arch"),
                cpu_model_name=data.get("cpu_name"),
                number_of_cpu_cores=data.get("cpu_cores_effective"),
                cpu_clock_speed_ghz=data.get("cpu_ghz"),
                ram_mb=data.get("cpu_ram"),
                disk_model_name=data.get("disk_name"),
                disk_space_gb=data.get("disk_space"),
                disk_bandwidth_mbytes_per_sec=data.get("disk_bw"),
                pcie_generation=data.get("pci_gen"),
                pcie_bandwidth_gbytes_per_sec=data.get("pcie_bw"),
                network_download_mbits_per_sec=data.get("inet_down"),
                network_upload_mbits_per_sec=data.get("inet_up"),
                reliability_score=data.get("reliability2"),
                deep_learning_score=data.get("dlperf"),
                geolocation=data.get("geolocation"),
                verification_flag=data.get("verification"),
                rentable_flag=data.get("rentable"),
                rented_flag=data.get("rented"),
            )
        except (KeyError, ValueError, TypeError, ValidationError) as e:
            self.logger.warning(f"Could not parse offer {data.get('id')}: {e}")
            return None


async def main(scheduled_at: str | None = None):
    loader: ConfigLoader = ConfigLoader()
    vast_ai_config: VastAIConfig = loader.get_vast_ai()
    compute_offers: Dataset = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
    if not vast_ai_config.enabled:
        return

    compute_offers_ingestor: ComputeOffersIngestor = ComputeOffersIngestor(
        dataset=compute_offers,
        config=vast_ai_config,
        lakehouse_config=loader.get_lakehouse(),
        bronze_table=BronzeTable(COMPUTE_OFFERS_BRONZE_SCHEMA, AppendByHour(column="snapshot_at")),
        http_config=loader.get_http(),
        snapshot_at=resolve_snapshot_at(scheduled_at),
    )
    logging.info(f"Starting source {compute_offers_ingestor.name}...")
    await compute_offers_ingestor.run()


def run(scheduled_at: str | None = None):
    asyncio.run(main(scheduled_at))


if __name__ == "__main__":
    run()
