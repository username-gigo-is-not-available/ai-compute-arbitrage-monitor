from common.enums import ConsumerCategoryType
from ingest.models.base import BaseRecord


class ElectricityTariffTier(BaseRecord):
    consumer_category: ConsumerCategoryType
    label: str
    metric: str
    value: str
    tariff_tier: str
    valid_from_text: str
