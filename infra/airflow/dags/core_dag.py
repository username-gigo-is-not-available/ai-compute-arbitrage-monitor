from __future__ import annotations

from datetime import datetime

from airflow import DAG

from transform_strategy import TransformStrategy
from marts_dag_factory import MartsDagFactory
from refine_strategy import RefineStrategy
from core_dag_factory import DagFactory
from pipeline_config import PipelineConfig
from common.enums import DatasetType, DatasetName
from config.loader import ConfigLoader


PIPELINE_CONFIGS: list[PipelineConfig] = [
    PipelineConfig(
        dataset_name=DatasetName.COMPUTE_OFFERS,
        dataset_type=DatasetType.SOURCES,
        schedule="@hourly",
        start_date=datetime(2026, 1, 1),
        description="Vast.ai compute offers → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.EXCHANGE_RATES,
        dataset_type=DatasetType.SOURCES,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="MKD/USD exchange rate → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS,
        dataset_type=DatasetType.SEEDS,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff tiers → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_BLOCKS,
        dataset_type=DatasetType.SEEDS,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff blocks → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_FEES,
        dataset_type=DatasetType.SEEDS,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff fees → Bronze → Silver → Gold",
    ),
    PipelineConfig(
        dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE,
        dataset_type=DatasetType.SEEDS,
        schedule="@daily",
        start_date=datetime(2026, 1, 1),
        description="EVN tariff window schedule → Bronze → Silver → Gold",
    ),
]

# ---------------------------------------------------------------------------
# Register DAGs
# ---------------------------------------------------------------------------

for pipeline_config in PIPELINE_CONFIGS:
    globals()[pipeline_config.dag_id] = DagFactory(
        pipeline_config=pipeline_config,
        refine_strategy=RefineStrategy.build_strategy(config=ConfigLoader()),
        transform_strategy=TransformStrategy.build_strategy(config=ConfigLoader()),
    ).build()

gpu_arbitrage__marts = MartsDagFactory(
    pipeline_configs=PIPELINE_CONFIGS,
    transform_strategy=TransformStrategy.build_strategy(config=ConfigLoader()),
).build()
