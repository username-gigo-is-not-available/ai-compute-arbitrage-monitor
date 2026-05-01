from __future__ import annotations

import os
from datetime import datetime

from infra.airflow.dags.marts_dag_factory import MartsDagFactory
from common.enums import DatasetType
from src.config.cluster import GCPClusterConfig
from src.config.loader import ConfigLoader
from src.config.storage import GCPStorageConfig
from infra.airflow.dags.dag_factory import DagFactory
from infra.airflow.dags.pipeline_config import PipelineConfig
from src.ingest.sources.vast_ai import run as vast_ai_run
from src.ingest.sources.exchange_rate import run as exchange_rate_run
from src.ingest.seeds.electricity_tariff_price import run as tariff_price_run
from src.ingest.seeds.electricity_tariff_schedule import run as tariff_schedule_run


CONFIG: ConfigLoader = ConfigLoader()
CLUSTER_CONFIG: GCPClusterConfig = CONFIG.get_cluster()
STORAGE_CONFIG: GCPStorageConfig = CONFIG.get_storage()
DBT_PROJECT_DIR: str = os.environ["DBT_PROJECT_DIR"]
DBT_TARGET_DIR: str = os.environ["DBT_TARGET_DIR"]

PIPELINE_CONFIGS: list[PipelineConfig] = [
    PipelineConfig(
        name="compute_offers",
        run_fn=vast_ai_run,
        schedule="@hourly",
        start_date=datetime(2025, 4, 1),
        description="Vast.ai compute offers → Bronze → Silver → Gold",
        dataset_type=DatasetType.SOURCES,
    ),
    PipelineConfig(
        name="exchange_rates",
        run_fn=exchange_rate_run,
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="MKD/USD exchange rate → Bronze → Silver → Gold",
        dataset_type=DatasetType.SOURCES,
    ),
    PipelineConfig(
        name="electricity_tariff_prices",
        run_fn=tariff_price_run,
        schedule="@daily",
        start_date=datetime(2025, 4, 1),
        description="EVN tariff prices → Bronze → Silver → Gold",
        dataset_type=DatasetType.SEEDS,
    ),
    PipelineConfig(
        name="electricity_tariffs_schedule",
        run_fn=tariff_schedule_run,
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
        cluster_config=CLUSTER_CONFIG,
        storage_config=STORAGE_CONFIG,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_target_dir=DBT_TARGET_DIR
    ).build()

gpu_arbitrage__marts = MartsDagFactory(
    pipeline_configs=PIPELINE_CONFIGS,
    dbt_project_dir=DBT_PROJECT_DIR,
    dbt_target_dir=DBT_TARGET_DIR
).build()
