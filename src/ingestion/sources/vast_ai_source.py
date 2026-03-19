import asyncio
import logging
from http import HTTPStatus
from typing import Any

from aiohttp import ClientSession
from pydantic import ValidationError

from config.loader import BronzeConfigLoader
from config.sources.vast_ai import VastAIConfig
from ingestion.base import StreamIngestor
from ingestion.models.vast_ai_offer import VastAIOffer
from pubsub.producer import KafkaProducer


class VastAISource(StreamIngestor):

    def __init__(self, config: VastAIConfig, name: str = None):
        super().__init__(config=config, name=name)

    async def poll(self) -> list[VastAIOffer]:
        async with ClientSession() as session:
            async with session.get(
                    self.config.url,
                    headers=self.config.header,
                    params=self.config.params
            ) as response:
                if response.status != HTTPStatus.OK:
                    self.logger.error(f"Vast.AI API returned HTTP {response.status}")
                    return []

                data: dict[str, Any] = await response.json(encoding="utf-8")
                offers = []
                for row in data.get("offers", []):
                    offer = self.parse(data=row)
                    if offer:
                        offers.append(offer)
                return offers

    def parse(self, **kwargs) -> VastAIOffer | None:
        data: dict[str, Any] = kwargs.get("data")
        try:
            return VastAIOffer(
                instance_id=data.get("id"),
                price_usd_per_hour=data.get("discounted_dph_total"),
                gpu_architecture=data.get("gpu_arch"),
                gpu_model_name=data.get("gpu_name"),
                gpu_memory_mb=data.get("gpu_ram"),
                gpu_max_power_watts=data.get("gpu_max_power"),
                number_of_gpus=data.get("num_gpus", 1),
                max_cuda_version_supported=data.get("cuda_max_good"),
                cpu_architecture=data.get("cpu_arch"),
                cpu_model_name=data.get("cpu_name"),
                number_of_cpu_cores=data.get("cpu_cores_effective"),
                ram_mb=data.get("cpu_ram"),
                disk_model_name=data.get("disk_name"),
                disk_space_gb=data.get("disk_space"),
                network_download_mbps=data.get("inet_down"),
                network_upload_mbps=data.get("inet_up"),
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

    def publish(self, producer: KafkaProducer, data: list[VastAIOffer]) -> None:
        for offer in data:
            producer.produce(
                topic=self.config.topic_name,
                payload=offer.model_dump(mode="json"),
                key=str(offer.instance_id)
            )
        producer.flush()
        self.logger.info(f"Published {len(data)} offers to '{self.config.topic_name}'")


async def main():
    loader: BronzeConfigLoader = BronzeConfigLoader()
    vast_ai_config: VastAIConfig = loader.get_vast_ai()
    if not vast_ai_config.enabled:
        return

    vast_ai: VastAISource = VastAISource(config=vast_ai_config)
    logging.info(f"Starting source {vast_ai.name}...")
    await vast_ai.run(producer=KafkaProducer(config=loader.get_kafka()))


if __name__ == "__main__":
    asyncio.run(main())
