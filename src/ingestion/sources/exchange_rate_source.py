import asyncio
import logging
import ssl
from datetime import datetime
from http import HTTPStatus
from typing import Any

import certifi
from aiohttp import ClientSession, ClientTimeout

from config.sources.exchange_rate import ExchangeRateConfig
from config.http import HttpConfig
from config.loader import BronzeConfigLoader
from pubsub.producer import KafkaProducer
from ingestion.models.exchange_rate import ExchangeRate
from ingestion.base import StreamIngestor
from serializers.json_serializer import JsonSerializer


class ExchangeRateSource(StreamIngestor):

    def __init__(self, config: ExchangeRateConfig, http_config: HttpConfig, name: str = None):
        super().__init__(config=config, name=name)
        self.http_config = http_config
        self.timestamp_format = "%a, %d %b %Y %H:%M:%S %z"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def poll(self) -> ExchangeRate | None:
        url: str = self.config.url
        timeout_seconds: int = self.http_config.timeout_seconds

        async with ClientSession() as session:
            async with session.get(url, ssl=self.ssl_context, timeout=ClientTimeout(total=timeout_seconds)) as response:
                status: int = response.status
                if status != HTTPStatus.OK:
                    self.logger.error(f"Exchange Rate API poll returned HTTP status: {status}.")
                    return None
                data: dict[str, Any] = await response.json(encoding="utf-8")
                return self.parse(data=data)

    def parse(self, **kwargs) -> ExchangeRate | None:
        from_currency: str = self.config.from_currency
        to_currency: str = self.config.to_currency
        data: dict[str, Any] = kwargs.get('data')
        try:
            return ExchangeRate(
                from_currency=from_currency,
                to_currency=to_currency,
                rate=float(data.get("conversion_rates").get(to_currency)),
                timestamp=datetime.strptime(data.get("time_last_update_utc"), self.timestamp_format)
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse exchange rate: {from_currency}/{to_currency}: {e}")
            return None

    def publish(self, producer: KafkaProducer, data: ExchangeRate) -> None:
        producer.produce(
            topic=self.config.topic_name,
            payload=JsonSerializer.serialize(data),
            key=f"{data.from_currency}_{data.to_currency}"
        )
        producer.flush()
        self.logger.info(
            f"Published {data.from_currency}/{data.to_currency} = {data.rate} to '{self.config.topic_name}'")


async def main():
    loader: BronzeConfigLoader = BronzeConfigLoader()
    exchange_rate_config: ExchangeRateConfig = loader.get_exchange_rate()
    if not exchange_rate_config.enabled:
        return

    exchange_rate: ExchangeRateSource = ExchangeRateSource(config=loader.get_exchange_rate(),
                                                           http_config=loader.get_http())
    logging.info(f"Starting source {exchange_rate.name}...")
    await exchange_rate.run(producer=KafkaProducer(config=loader.get_kafka()))


if __name__ == '__main__':
    asyncio.run(main())
