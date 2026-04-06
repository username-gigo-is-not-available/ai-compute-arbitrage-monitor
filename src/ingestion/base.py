import asyncio
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from abc import ABC, abstractmethod
from typing import Any

from ingestion.models.electricity_tariff_price import ElectricityTariffPrice
from ingestion.models.electricity_tariff_schedule import ElectricityTariffSchedule
from ingestion.models.types import IngestorConfig
from ingestion.models.vast_ai_offer import VastAIOffer
from ingestion.models.exchange_rate import ExchangeRate
from pubsub.producer import KafkaProducer
from serializers.json_serializer import JsonSerializer


class StreamIngestor(ABC):

    def __init__(self, config: IngestorConfig, name: str = None):
        self.name = name or self.__class__.__name__
        self.config = config
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    async def poll(self) -> list[VastAIOffer] | ExchangeRate | None:
        raise NotImplementedError

    @abstractmethod
    def parse(self, **kwargs) -> VastAIOffer | ExchangeRate | None:
        raise NotImplementedError

    @abstractmethod
    def publish(self, producer: KafkaProducer, data: list[VastAIOffer] | ExchangeRate) -> None:
        raise NotImplementedError

    async def ingest(self, producer: KafkaProducer) -> None:
        self.logger.info(f"Starting poll for {self.name}...")
        data = await self.poll()
        if data:
            self.publish(producer, data)
        else:
            self.logger.warning(f"{self.name} returned no data, skipping publish.")

    async def run(self, producer: KafkaProducer) -> None:
        try:
            await self.ingest(producer)
        except Exception as e:
            self.logger.error(f"Unexpected error in {self.name}: {e}", exc_info=True)


class BatchIngestor(ABC):

    def __init__(self, config: IngestorConfig, name: str = None):
        self.name = name or self.__class__.__name__
        self.config = config
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    def load(self) -> list[ElectricityTariffPrice | ElectricityTariffSchedule]:
        raise NotImplementedError

    @abstractmethod
    def parse(self, **kwargs) -> ElectricityTariffPrice | ElectricityTariffSchedule | None:
        raise NotImplementedError

    def store(self, data: list[ElectricityTariffPrice | ElectricityTariffSchedule]) -> None:
        self.config.output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path: Path = self.config.output_path_with_suffix(".parquet")
        records: list[dict[str, Any]] = JsonSerializer.serialize_batch(data)
        table = pa.Table.from_pylist(records)
        pq.write_table(table, output_path)
        self.logger.info(f"Written {len(table)} {self.name} records to {output_path}")

    def run(self) -> None:
        self.logger.info(f"Starting load for {self.name}...")
        data = self.load()
        if data:
            self.store(data)
        else:
            self.logger.warning(f"{self.name} returned no data, skipping store.")
