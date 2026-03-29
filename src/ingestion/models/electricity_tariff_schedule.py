from ingestion.models.base import BaseRecord
from ingestion.models.enums import TariffType


class ElectricityTariffSchedule(BaseRecord):
    tariff_type: TariffType
    day_of_week: int
    start_hour: int
    end_hour: int
    valid_from: str
