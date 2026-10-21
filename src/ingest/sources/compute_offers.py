import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any

from pydantic import ValidationError

from common.classes import Dataset, Snapshot
from common.enums import OfferType, DatasetType, DatasetName
from config.apis.vast_ai import VastAIConfig
from config.loader import ConfigLoader
from ingest.base import Ingestor
from ingest.models.vast_ai_offer import VastAIOffer
from ingest.schemas.compute_offers import COMPUTE_OFFERS_BRONZE_SCHEMA
from ingest.write_strategy import AppendByDay, BronzeTable


@dataclass
class ComputeOffersIngestor(Ingestor):
    snapshot: Snapshot
    rows_used: int = field(init=False, default=0)
    requests: int = field(init=False, default=0)

    def run(self) -> None:
        if self.snapshot.is_backfill():
            self.logger.warning(
                f"Skipping {self.name}: snapshot_at {self.snapshot.at.isoformat()} is in the past and "
                f"Vast.ai only serves the current market, so it cannot be backfilled (ADR-019)."
            )
            return
        super().run()

    def load(self) -> list[VastAIOffer]:
        ingested_at: datetime = datetime.now(UTC)
        rows = self.fetch_census()
        self.logger.info(f"Census of {len(rows)} offers used {self.rows_used} rows in {self.requests} requests")
        offers = [self.parse(data=row, ingested_at=ingested_at, offer_type=OfferType.ON_DEMAND) for row in rows]
        return [offer for offer in offers if offer]

    def fetch_census(self) -> list[dict[str, Any]]:
        # /bundles returns a random sample when more offers match than the limit (ADR-020). One request for the whole
        # market gives that sample; it is the map for splitting the market into ranges small enough to come back whole.
        data = self.request_range(start=0, end=None)
        if len(data) < self.config.split_at:
            return data
        rows = []
        sample_ids = [row["ask_contract_id"] for row in data]
        for start, end in self.split_range(sample_ids):
            data = self.request_range(start, end)
            if len(data) >= self.config.split_at:
                raise RuntimeError(f"Range [{start}, {end}) came back full, so it may be a sample; raise census_parts")
            rows += data
        return rows

    def split_range(self, sample_ids: list[int]) -> list[tuple[int, int | None]]:
        # A cut after every step-th sampled id: the sample is densest where offers are, so each range holds about
        # market size / census_parts offers. The first cut is after the smallest id, so no range is empty.
        ids = sorted(sample_ids)
        step = max(1, len(ids) // self.config.census_parts)
        cuts = ids[step::step]
        bounds = [0, *cuts, None]
        return list(zip(bounds, bounds[1:]))

    def request_range(self, start: int, end: int | None) -> list[dict[str, Any]]:
        data: dict[str, Any] = self.fetch(self.config.url, headers=self.config.header,
                                          params=self.config.params(start, end)).json()
        time.sleep(self.config.request_interval_seconds)
        offers = data.get("offers", [])
        self.requests += 1
        self.rows_used += len(offers)
        self.logger.info(f"Range [{start}, {end}): {len(offers)} offers; {self.rows_used} rows used in {self.requests} requests")
        return offers

    def parse(self, data: dict[str, Any], ingested_at: datetime, offer_type: OfferType) -> VastAIOffer | None:
        try:
            return VastAIOffer(
                snapshot_at=self.snapshot.at,
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
                number_of_offer_gpus=data.get("num_gpus", 1),
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


def main(scheduled_at: str | None = None) -> None:
    loader: ConfigLoader = ConfigLoader()
    vast_ai_config: VastAIConfig = loader.get_vast_ai()
    compute_offers: Dataset = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
    if not vast_ai_config.enabled:
        return

    compute_offers_ingestor: ComputeOffersIngestor = ComputeOffersIngestor(
        dataset=compute_offers,
        config=vast_ai_config,
        lakehouse_config=loader.get_lakehouse(),
        bronze_table=BronzeTable(COMPUTE_OFFERS_BRONZE_SCHEMA, AppendByDay(column="snapshot_at")),
        http_config=loader.get_http(),
        snapshot=Snapshot.from_scheduled_at(scheduled_at),
    )
    logging.info(f"Starting source {compute_offers_ingestor.name}...")
    compute_offers_ingestor.run()


def run(scheduled_at: str | None = None) -> None:
    main(scheduled_at)


if __name__ == "__main__":
    run()
