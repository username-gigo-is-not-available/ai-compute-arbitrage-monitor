import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, UTC
from http import HTTPStatus
from typing import Any

from aiohttp import ClientSession
from pydantic import ValidationError

from common.classes import Dataset
from config.loader import ConfigLoader
from config.apis.vast_ai import VastAIConfig
from config.storage import GCPStorageConfig
from ingest.base import AsyncIngestor
from common.enums import OfferType, DatasetType, DatasetName
from ingest.models.vast_ai_offer import VastAIOffer

@dataclass
class ComputeOffersIngestor(AsyncIngestor):

    async def load(self) -> list[VastAIOffer]:
        async with ClientSession() as session:
            offers = []
            for offer_type in OfferType:
                async with session.get(
                        self.config.url,
                        headers=self.config.header,
                        params=self.config.params(offer_type=offer_type)
                ) as response:
                    if response.status != HTTPStatus.OK:
                        self.logger.error(f"Vast.AI API returned HTTP {response.status}")
                        return []

                    data: dict[str, Any] = await response.json(encoding="utf-8")
                    ingested_at: datetime = datetime.now(UTC)
                    for row in data.get("offers", []):
                        offer = self.parse(data=row, timestamp=ingested_at, offer_type=offer_type)
                        if offer:
                            offers.append(offer)

            return offers

    def parse(self, **kwargs) -> VastAIOffer | None:
        data: dict[str, Any] = kwargs.get("data")
        ingested_at: datetime = kwargs.get("timestamp")
        offer_type: OfferType = kwargs.get("offer_type")
        try:
            return VastAIOffer(
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


async def main():
    loader: ConfigLoader = ConfigLoader()
    vast_ai_config: VastAIConfig = loader.get_vast_ai()
    storage_config: GCPStorageConfig = loader.get_storage()
    compute_offers: Dataset = Dataset(dataset_name=DatasetName.COMPUTE_OFFERS, dataset_type=DatasetType.SOURCES)
    if not vast_ai_config.enabled:
        return

    compute_offers_ingestor: ComputeOffersIngestor = ComputeOffersIngestor(dataset=compute_offers, config=vast_ai_config, storage_config=storage_config)
    logging.info(f"Starting source {compute_offers_ingestor.name}...")
    await compute_offers_ingestor.run()


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
