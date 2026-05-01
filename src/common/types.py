from config.seeds.erc import ERCConfig
from config.seeds.evn import EVNConfig
from config.sources.exchange_rate import ExchangeRateConfig
from config.sources.vast_ai import VastAIConfig

DatasetConfig = VastAIConfig | ExchangeRateConfig | ERCConfig | EVNConfig
