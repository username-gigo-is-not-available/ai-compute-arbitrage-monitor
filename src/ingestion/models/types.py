from config.seeds.erc import ERCConfig
from config.seeds.evn import EVNConfig
from config.sources.exchange_rate import ExchangeRateConfig
from config.seeds.open_db import OpenDBConfig
from config.sources.vast_ai import VastAIConfig
from ingestion.models.hardware_specification import GPUSpecification, CPUSpecification

IngestorConfig = VastAIConfig | ExchangeRateConfig | ERCConfig | OpenDBConfig | EVNConfig
HardwareSpecification = GPUSpecification | CPUSpecification
BatchConfig = ERCConfig | OpenDBConfig | EVNConfig
StreamConfig = VastAIConfig | ExchangeRateConfig