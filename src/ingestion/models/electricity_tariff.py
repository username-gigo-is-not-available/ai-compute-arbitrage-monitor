
from ingestion.models.base import BaseRecord


class ElectricityTariff(BaseRecord):
    tariff_type: str
    price_per_kwh_mkd: str
    valid_from: str
