import asyncio
import logging
import ssl
from dataclasses import dataclass, field
from datetime import datetime, UTC
from http import HTTPStatus
from typing import Any

import certifi
from aiohttp import ClientSession, ClientTimeout

from common.classes import Dataset
from common.enums import DatasetType, DatasetName
from config.apis.exchange_rate import ExchangeRateConfig
from config.http import HttpConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from ingest.base import AsyncIngestor
from ingest.models.exchange_rate import ExchangeRate


@dataclass
class ExchangeRateSource(AsyncIngestor):
    http_config: HttpConfig
    timestamp_format: str = field(init=False)
    ssl_context: ssl.SSLContext = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self.timestamp_format = "%a, %d %b %Y %H:%M:%S %z"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def load(self) -> list[ExchangeRate]:
        url: str = self.config.url
        timeout_seconds: int = self.http_config.timeout_seconds

        async with ClientSession() as session:
            async with session.get(url, ssl=self.ssl_context, timeout=ClientTimeout(total=timeout_seconds)) as response:
                status: int = response.status
                if status != HTTPStatus.OK:
                    self.logger.error(f"Exchange Rate API poll returned HTTP status: {status}.")
                    return []
                data: dict[str, Any] = await response.json(encoding="utf-8")
                return [self.parse(data=data)]

    def parse(self, **kwargs) -> ExchangeRate | None:
        from_currency: str = self.config.from_currency
        to_currency: str = self.config.to_currency
        data: dict[str, Any] = kwargs.get('data')
        try:
            return ExchangeRate(
                ingested_at=datetime.now(UTC),
                from_currency=from_currency,
                to_currency=to_currency,
                value=float(data.get("conversion_rates").get(to_currency)),
                timestamp=datetime.strptime(data.get("time_last_update_utc"), self.timestamp_format)
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse exchange rate: {from_currency}/{to_currency}: {e}")
            return None


async def main():
    loader: ConfigLoader = ConfigLoader()
    exchange_rate_config: ExchangeRateConfig = loader.get_exchange_rate()
    storage_config: GCPStorageConfig = loader.get_storage()
    exchange_rates: Dataset = Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES)
    if not exchange_rate_config.enabled:
        return

    exchange_rate = ExchangeRateSource(
        dataset=exchange_rates,
        config=exchange_rate_config,
        storage_config=storage_config,
        http_config=loader.get_http(),
    )
    logging.info(f"Starting source {exchange_rate.name}...")
    await exchange_rate.run()


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()