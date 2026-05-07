from __future__ import annotations

from datetime import datetime

from infra.airflow.dags.marts_dag_factory import MartsDagFactory
from common.enums import DatasetType
from infra.airflow.dags.refine_strategy import RefineStrategy
from infra.airflow.dags.transform_adapter import DbtAdapter
from src.config.loader import ConfigLoader
from infra.airflow.dags.dag_factory import DagFactory
from infra.airflow.dags.pipeline_config import PipelineConfig
from src.ingest.sources.vast_ai import run as vast_ai_ingest_run
from src.ingest.sources.exchange_rate import run as exchange_rate_ingest_run
from src.ingest.seeds.electricity_tariff_price import run as tariff_price_ingest_run
from src.ingest.seeds.electricity_tariff_schedule import run as tariff_schedule_ingest_run
from src.refine.pipelines.compute_offers import run as compute_offer_refine_run
from src.refine.pipelines.exchange_rates import run as exchange_rate_refine_run
from src.refine.pipelines.electricity_tariff_prices import run as tariff_price_refine_run
from src.refine.pipelines.electricity_tariffs_schedule import run as tariff_schedule_refine_run

CONFIG: ConfigLoader = ConfigLoader()

PIPELINE_CONFIGS: list[PipelineConfig] = [
    PipelineConfig(
        name="compute_offers",
        ingest_fn=vast_ai_ingest_run,
        refine_fn=compute_offer_refine_run,
        schedule="@hourly",
        start_date=datetime(2025, 4, 1),
        description="Vast.ai compute offers → Bronze → Silver → Gold",
        dataset_type=DatasetType.SOURCES,
    ),
    PipelineConfig(
        name="exchange_rates",
        ingest_fn=exchange_rate_ingest_run,
        refine_fn=exchange_rate_refine_run,
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="MKD/USD exchange rate → Bronze → Silver → Gold",
        dataset_type=DatasetType.SOURCES,
    ),
    PipelineConfig(
        name="electricity_tariff_prices",
        ingest_fn=tariff_price_ingest_run,
        refine_fn=tariff_price_refine_run,
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="EVN tariff prices → Bronze → Silver → Gold",
        dataset_type=DatasetType.SEEDS,
    ),
    PipelineConfig(
        name="electricity_tariffs_schedule",
        ingest_fn=tariff_schedule_ingest_run,
        refine_fn=tariff_schedule_refine_run,
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="EVN tariff schedule → Bronze → Silver → Gold",
        dataset_type=DatasetType.SEEDS
    ),
]

# ---------------------------------------------------------------------------
# Register DAGs
# ---------------------------------------------------------------------------

for pipeline_config in PIPELINE_CONFIGS:
    globals()[pipeline_config.dag_id] = DagFactory(
        pipeline_config=pipeline_config,
        refine_strategy=RefineStrategy.build_strategy(config=CONFIG),
        dbt=DbtAdapter()
    ).build()

gpu_arbitrage__marts = MartsDagFactory(
    pipeline_configs=PIPELINE_CONFIGS,
    dbt=DbtAdapter()
).build()
