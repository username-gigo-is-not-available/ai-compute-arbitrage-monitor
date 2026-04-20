
from ingest.models.base import BaseRecord


class ElectricityTariffPrice(BaseRecord):
    tariff_description: str
    price_mkd_per_kwh: str
    valid_from: str
