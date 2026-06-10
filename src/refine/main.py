import logging

from common.enums import DataStageType
from config.loader import ConfigLoader
from refine.init import initialize_spark
from refine.seeds.electricity_tariff_tiers import ElectricityTariffTiersPipeline
from refine.seeds.electricity_tariff_schedule import ElectricityTariffSchedulePipeline
from refine.sources.exchange_rates import ExchangeRatesPipeline
from refine.sources.compute_offers import ComputeOffersPipeline
from refine.schemas.compute_offers import COMPUTE_OFFER_SCHEMA
from refine.schemas.electricity_tariff_tiers import ELECTRICITY_TARIFF_TIERS_SCHEMA
from refine.schemas.electricity_tariff_schedule import ELECTRICITY_TARIFF_SCHEDULE_SCHEMA
from refine.schemas.exchange_rate import EXCHANGE_RATE_SCHEMA

if __name__ == "__main__":
    session = initialize_spark()
    try:
        loader = ConfigLoader()

        erc_config = loader.get_erc(DataStageType.SILVER)
        evn_config = loader.get_evn(DataStageType.SILVER)
        vast_ai_config = loader.get_vast_ai(DataStageType.SILVER)
        exchange_rate_config = loader.get_exchange_rate(DataStageType.SILVER)

        pipelines = [
            (erc_config,
             ElectricityTariffTiersPipeline(session=session, schema=ELECTRICITY_TARIFF_TIERS_SCHEMA, config=erc_config)
             ),
            (evn_config, ElectricityTariffSchedulePipeline(session=session, schema=ELECTRICITY_TARIFF_SCHEDULE_SCHEMA,
                                                           config=evn_config)),
            (vast_ai_config,
             ComputeOffersPipeline(session=session, schema=COMPUTE_OFFER_SCHEMA, config=vast_ai_config)),
            (exchange_rate_config,
             ExchangeRatesPipeline(session=session, schema=EXCHANGE_RATE_SCHEMA, config=exchange_rate_config)),
        ]

        for config, pipeline in pipelines:
            if config.enabled:
                pipeline.run()

    except Exception as e:
        logging.error(e)
    finally:
        session.stop()
