from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ingest.base import Ingestor
from ingest.models.exchange_rate import ExchangeRate


@dataclass
class ExchangeRateIngestor(Ingestor):
    timestamp_format: str = field(init=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.timestamp_format = "%a, %d %b %Y %H:%M:%S %z"

    def load(self) -> list[ExchangeRate]:
        data: dict[str, Any] = self.http.get(self.config.url).json()
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
