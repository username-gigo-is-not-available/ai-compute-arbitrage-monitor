import logging
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any

from common.classes import Dataset
from common.enums import DatasetType, DatasetName
from config.apis.exchange_rate import ExchangeRateConfig
from config.loader import ConfigLoader
from ingest.base import Ingestor
from ingest.models.exchange_rate import ExchangeRate
from ingest.schemas.exchange_rates import EXCHANGE_RATES_BRONZE_SCHEMA
from ingest.write_strategy import AppendByDay, BronzeTable


@dataclass
class ExchangeRateIngestor(Ingestor):
    timestamp_format: str = field(init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.timestamp_format = "%a, %d %b %Y %H:%M:%S %z"

    def load(self) -> list[ExchangeRate]:
        data: dict[str, Any] = self.fetch(self.config.url).json()
        return [self.parse(data=data)]

    def parse(self, data: dict[str, Any]) -> ExchangeRate:
        to_currency: str = self.config.to_currency
        return ExchangeRate(
            ingested_at=datetime.now(UTC),
            from_currency=self.config.from_currency,
            to_currency=to_currency,
            value=float(data["conversion_rates"][to_currency]),
            timestamp=datetime.strptime(data["time_last_update_utc"], self.timestamp_format)
        )


def main() -> None:
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
    exchange_rate_ingestor.run()


def run(scheduled_at: str | None = None) -> None:
    main()


if __name__ == "__main__":
    run()