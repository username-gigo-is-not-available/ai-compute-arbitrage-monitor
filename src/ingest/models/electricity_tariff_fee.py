from common.enums import ConsumerCategoryType
from ingest.models.base import BaseRecord


class ElectricityTariffFee(BaseRecord):
    consumer_category: ConsumerCategoryType
    label: str
    metric: str
    value: str
    valid_from_text: str
