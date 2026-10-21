import asyncio
import logging
import ssl
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any

import certifi
from aiohttp import ClientError, ClientSession, ClientTimeout

from common.classes import Dataset
from common.enums import DatasetType, DatasetName
from config.apis.exchange_rate import ExchangeRateConfig
from config.loader import ConfigLoader
from ingest.base import AsyncIngestor
from ingest.models.exchange_rate import ExchangeRate
from ingest.schemas.exchange_rates import EXCHANGE_RATES_BRONZE_SCHEMA
from ingest.write_strategy import AppendByDay, BronzeTable


@dataclass
class ExchangeRateIngestor(AsyncIngestor):
    timestamp_format: str = field(init=False)
    ssl_context: ssl.SSLContext = field(init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.timestamp_format = "%a, %d %b %Y %H:%M:%S %z"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def load(self) -> list[ExchangeRate]:
        url: str = self.config.url
        timeout_seconds: int = self.http_config.timeout_seconds

        async with ClientSession() as session:

            response = await self.fetch_async(
                exception_type=(ClientError, asyncio.TimeoutError),
                http_get_function=session.get,
                url=url,
                ssl=self.ssl_context,
                timeout=ClientTimeout(total=timeout_seconds),
            )
            response.raise_for_status()
            try:
                data: dict[str, Any] = await response.json(encoding="utf-8")
                return [self.parse(data=data)]
            finally:
                response.close()

    def parse(self, **kwargs: Any) -> ExchangeRate:
        to_currency: str = self.config.to_currency
        data: dict[str, Any] = kwargs.get('data')
        return ExchangeRate(
            ingested_at=datetime.now(UTC),
            from_currency=self.config.from_currency,
            to_currency=to_currency,
            value=float(data["conversion_rates"][to_currency]),
            timestamp=datetime.strptime(data["time_last_update_utc"], self.timestamp_format)
        )


async def main() -> None:
    loader: ConfigLoader = ConfigLoader()
    exchange_rate_config: ExchangeRateConfig = loader.get_exchange_rate()
    exchange_rates: Dataset = Dataset(dataset_name=DatasetName.EXCHANGE_RATES, dataset_type=DatasetType.SOURCES)
    if not exchange_rate_config.enabled:
        return

    exchange_rate_ingestor = ExchangeRateIngestor(
        dataset=exchange_rates,
        config=exchange_rate_config,
        lakehouse_config=loader.get_lakehouse(),
        bronze_table=BronzeTable(EXCHANGE_RATES_BRONZE_SCHEMA, AppendByDay(column="timestamp")),
        http_config=loader.get_http(),
    )
    logging.info(f"Starting source {exchange_rate_ingestor.name}...")
    await exchange_rate_ingestor.run()


def run(scheduled_at: str | None = None) -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()