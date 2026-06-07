import asyncio
import logging

from common.enums import DataStageType
from config.loader import ConfigLoader
from ingest.seeds.electricity_tariffs_schedule import ElectricityTariffScheduleSeed
from ingest.seeds.electricity_tariff_prices import ElectricityTariffPricesSeed
from ingest.sources.exchange_rates import ExchangeRateSource
from ingest.sources.compute_offers import VastAISource


async def main():
    loader = ConfigLoader()

    erc_config = loader.get_erc(DataStageType.BRONZE)
    evn_config = loader.get_evn(DataStageType.BRONZE)
    vast_ai_config = loader.get_vast_ai(DataStageType.BRONZE)
    exchange_rate_config = loader.get_exchange_rate(DataStageType.BRONZE)

    async_sources = [
        (exchange_rate_config, ExchangeRateSource(config=exchange_rate_config, http_config=loader.get_http())),
        (vast_ai_config, VastAISource(config=vast_ai_config)),
    ]

    seeds = [
        (erc_config, ElectricityTariffPricesSeed(config=erc_config, http_config=loader.get_http())),
        (evn_config, ElectricityTariffScheduleSeed(config=evn_config, http_config=loader.get_http())),
    ]

    for config, source in async_sources:
        if config.enabled:
            logging.info(f"Starting source {source.name}...")
            await source.run()

    for config, seed in seeds:
        if config.enabled:
            logging.info(f"Starting seed {seed.name}...")
            seed.run()


if __name__ == '__main__':
    asyncio.run(main())