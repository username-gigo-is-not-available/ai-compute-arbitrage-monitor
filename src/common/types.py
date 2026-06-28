from config.apis.evn import EVNConfig
from config.apis.exchange_rate import ExchangeRateConfig
from config.apis.vast_ai import VastAIConfig

DatasetConfig = VastAIConfig | ExchangeRateConfig | EVNConfig
