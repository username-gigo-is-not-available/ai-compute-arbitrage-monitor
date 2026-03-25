
from ingestion.models.base import BaseRecord


class ElectricityTariff(BaseRecord):
    tariff_description: str
    price_per_kwh_mkd: str
    valid_from: str
