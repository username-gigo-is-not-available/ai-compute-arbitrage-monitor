from ingest.models.electricity_tariff_price import ElectricityTariffPrice
from ingest.models.electricity_tariff_schedule import ElectricityTariffSchedule
from ingest.models.exchange_rate import ExchangeRate
from ingest.models.vast_ai_offer import VastAIOffer

IngestorRecord = VastAIOffer | ExchangeRate | ElectricityTariffPrice | ElectricityTariffSchedule
