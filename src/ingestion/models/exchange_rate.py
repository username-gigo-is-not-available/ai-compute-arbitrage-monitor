from datetime import datetime

from ingestion.models.base import BaseRecord


class ExchangeRate(BaseRecord):
    from_currency: str
    to_currency: str
    rate: float
    rate_timestamp: datetime

