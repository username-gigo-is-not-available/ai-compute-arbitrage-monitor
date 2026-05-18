from __future__ import annotations

from datetime import datetime

from airflow import DAG

from marts_dag_factory import MartsDagFactory
from refine_strategy import RefineStrategy
from transform_adapter import DbtAdapter
from core_dag_factory import DagFactory
from pipeline_config import PipelineConfig
from common.enums import DatasetType
from config.loader import ConfigLoader
from ingest.sources.vast_ai import run as vast_ai_ingest_run
from ingest.sources.exchange_rate import run as exchange_rate_ingest_run
from ingest.seeds.electricity_tariff_price import run as tariff_price_ingest_run
from ingest.seeds.electricity_tariff_schedule import run as tariff_schedule_ingest_run


def make_refine_fn(module_path: str):
    def refine_fn():
        from importlib import import_module
        import_module(module_path).run()
    return refine_fn

PIPELINE_CONFIGS: list[PipelineConfig] = [
    PipelineConfig(
        name="compute_offers",
        ingest_fn=vast_ai_ingest_run,
        refine_fn=make_refine_fn("refine.pipelines.compute_offers"),
        schedule="@hourly",
        start_date=datetime(2025, 4, 1),
        description="Vast.ai compute offers → Bronze → Silver → Gold",
        dataset_type=DatasetType.SOURCES,
    ),
    PipelineConfig(
        name="exchange_rates",
        ingest_fn=exchange_rate_ingest_run,
        refine_fn=make_refine_fn("refine.pipelines.exchange_rates"),
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="MKD/USD exchange rate → Bronze → Silver → Gold",
        dataset_type=DatasetType.SOURCES,
    ),
    PipelineConfig(
        name="electricity_tariff_prices",
        ingest_fn=tariff_price_ingest_run,
        refine_fn=make_refine_fn("refine.pipelines.electricity_tariff_prices"),
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="EVN tariff prices → Bronze → Silver → Gold",
        dataset_type=DatasetType.SEEDS,
    ),
    PipelineConfig(
        name="electricity_tariffs_schedule",
        ingest_fn=tariff_schedule_ingest_run,
        refine_fn=make_refine_fn("refine.pipelines.electricity_tariffs_schedule"),
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="EVN tariff schedule → Bronze → Silver → Gold",
        dataset_type=DatasetType.SEEDS,
    ),
]

# ---------------------------------------------------------------------------
# Register DAGs
# ---------------------------------------------------------------------------

for pipeline_config in PIPELINE_CONFIGS:
    globals()[pipeline_config.dag_id] = DagFactory(
        pipeline_config=pipeline_config,
        refine_strategy=RefineStrategy.build_strategy(config=ConfigLoader()),
        dbt=DbtAdapter()
    ).build()

gpu_arbitrage__marts = MartsDagFactory(
    pipeline_configs=PIPELINE_CONFIGS,
    dbt=DbtAdapter()
).build()