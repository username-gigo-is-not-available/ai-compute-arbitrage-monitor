from common.enums import ConsumerCategoryType, TariffWindowType
from ingest.models.base import BaseRecord


class ElectricityTariffBlock(BaseRecord):
    consumer_category: ConsumerCategoryType
    tariff_window: TariffWindowType
    block_number_text: str
    kwh_boundaries_text: str
    valid_from_text: str