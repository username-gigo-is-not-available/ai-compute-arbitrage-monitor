import logging

from config.loader import SilverConfigLoader
from refine.init import initialize_spark
from refine.pipelines.electricity_tariff_prices import ElectricityTariffPricesPipeline
from refine.pipelines.electricity_tariffs_schedule import ElectricityTariffsSchedulePipeline
from refine.pipelines.exchange_rates import ExchangeRatesPipeline
from refine.pipelines.compute_offers import ComputeOffersPipeline
from refine.schemas.compute_offers import COMPUTE_OFFER_SCHEMA
from refine.schemas.electricity_tariff import ELECTRICITY_TARIFF_SCHEMA
from refine.schemas.electricity_tariff_schedule import ELECTRICITY_TARIFF_SCHEDULE_SCHEMA
from refine.schemas.exchange_rate import EXCHANGE_RATE_SCHEMA

if __name__ == "__main__":
    session = initialize_spark()
    try:
        loader = SilverConfigLoader()

        erc_config = loader.get_erc()
        evn_config = loader.get_evn()
        vast_ai_config = loader.get_vast_ai()
        exchange_rate_config = loader.get_exchange_rate()

        pipelines = [
            (erc_config,
             ElectricityTariffPricesPipeline(session=session, schema=ELECTRICITY_TARIFF_SCHEMA, config=erc_config)
             ),
            (evn_config, ElectricityTariffsSchedulePipeline(session=session, schema=ELECTRICITY_TARIFF_SCHEDULE_SCHEMA,
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
