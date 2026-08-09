from ingest.models.base import BaseRecord


class ElectricityTariffWindowSchedule(BaseRecord):
    schedule_text: str
    valid_from_text: str
