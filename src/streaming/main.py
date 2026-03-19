from pyspark.sql.streaming import StreamingQuery

from config.loader import SilverConfigLoader
from streaming.init import initialize_spark
from streaming.pipelines.cpu_specification import CPUPipeline
from streaming.pipelines.electricity_tariffs import ElectricityTariffsPipeline
from streaming.pipelines.electricity_tariffs_schedule import ElectricityTariffsSchedulePipeline
from streaming.pipelines.exchange_rate import ExchangeRatePipeline
from streaming.pipelines.gpu_specification import GPUPipeline
from streaming.pipelines.vast_ai import VastAIOffersPipeline
from streaming.schemas import (
    CPU_SPECIFICATION_SCHEMA,
    GPU_SPECIFICATION_SCHEMA,
    ELECTRICITY_TARIFF_SCHEMA,
    ELECTRICITY_TARIFF_SCHEDULE_SCHEMA,
    COMPUTE_OFFER_SCHEMA,
    EXCHANGE_RATE_SCHEMA,
)

if __name__ == "__main__":
    session = initialize_spark()
    loader = SilverConfigLoader()
    kafka_config = loader.get_kafka()

    cpu_config = loader.get_open_db_cpu()
    gpu_config = loader.get_open_db_gpu()
    erc_config = loader.get_erc()
    evn_config = loader.get_evn()
    vast_ai_config = loader.get_vast_ai()
    exchange_rate_config = loader.get_exchange_rate()

    batch_pipelines = [
        (cpu_config, CPUPipeline(session=session, schema=CPU_SPECIFICATION_SCHEMA, config=cpu_config)),
        (gpu_config, GPUPipeline(session=session, schema=GPU_SPECIFICATION_SCHEMA, config=gpu_config)),
        (erc_config, ElectricityTariffsPipeline(session=session, schema=ELECTRICITY_TARIFF_SCHEMA, config=erc_config)),
        (evn_config, ElectricityTariffsSchedulePipeline(session=session, schema=ELECTRICITY_TARIFF_SCHEDULE_SCHEMA,
                                                        config=evn_config)),
    ]

    stream_pipelines = [
        (vast_ai_config, VastAIOffersPipeline(session=session, schema=COMPUTE_OFFER_SCHEMA, config=vast_ai_config,
                                              kafka_config=kafka_config)),
        (exchange_rate_config,
         ExchangeRatePipeline(session=session, schema=EXCHANGE_RATE_SCHEMA, config=exchange_rate_config,
                              kafka_config=kafka_config)),
    ]

    for config, pipeline in batch_pipelines:
        if config.enabled:
            pipeline.run()

    for config, pipeline in stream_pipelines:
        if config.enabled:
            pipeline.run()

    session.streams.awaitAnyTermination()
