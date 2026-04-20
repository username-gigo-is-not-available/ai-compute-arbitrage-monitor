from config.seeds.erc import ERCConfig
from config.seeds.evn import EVNConfig
from config.sources.exchange_rate import ExchangeRateConfig
from config.sources.vast_ai import VastAIConfig
from ingest.models.electricity_tariff_price import ElectricityTariffPrice
from ingest.models.electricity_tariff_schedule import ElectricityTariffSchedule
from ingest.models.exchange_rate import ExchangeRate
from ingest.models.vast_ai_offer import VastAIOffer

DatasetConfig = VastAIConfig | ExchangeRateConfig | ERCConfig | EVNConfig
IngestorRecord = VastAIOffer | ExchangeRate | ElectricityTariffPrice | ElectricityTariffSchedule
