import asyncio
import logging

from config.loader import BronzeConfigLoader
from ingestion.models.enums import HardwareComponentType
from ingestion.seeds.electricity_tariff_schedule_seed import ElectricityTariffScheduleSeed
from ingestion.seeds.electricity_tarrif_seed import ElectricityTariffSeed
from ingestion.seeds.hardware_specification_seed import HardwareSpecificationSeed
from ingestion.sources.exchange_rate_source import ExchangeRateSource
from ingestion.sources.vast_ai_source import VastAISource
from pubsub.producer import KafkaProducer


async def main():
    loader = BronzeConfigLoader()
    kafka_config = loader.get_kafka()

    cpu_config = loader.get_open_db_cpu()
    gpu_config = loader.get_open_db_gpu()
    erc_config = loader.get_erc()
    evn_config = loader.get_evn()
    vast_ai_config = loader.get_vast_ai()
    exchange_rate_config = loader.get_exchange_rate()

    producer = KafkaProducer(config=kafka_config)

    async_sources = [
        (exchange_rate_config, ExchangeRateSource(config=exchange_rate_config, http_config=loader.get_http())),
        (vast_ai_config, VastAISource(config=vast_ai_config)),
    ]

    seeds = [
        (cpu_config, HardwareSpecificationSeed(config=cpu_config, hardware_component_type=HardwareComponentType.CPU)),
        (gpu_config, HardwareSpecificationSeed(config=gpu_config, hardware_component_type=HardwareComponentType.GPU)),
        (erc_config, ElectricityTariffSeed(config=erc_config, http_config=loader.get_http())),
        (evn_config, ElectricityTariffScheduleSeed(config=evn_config, http_config=loader.get_http())),
    ]

    for config, source in async_sources:
        if config.enabled:
            logging.info(f"Starting source {source.name}...")
            await source.run(producer=producer)

    for config, seed in seeds:
        if config.enabled:
            logging.info(f"Starting seed {seed.name}...")
            seed.run()


if __name__ == '__main__':
    asyncio.run(main())