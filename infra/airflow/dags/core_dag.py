from __future__ import annotations

from airflow import DAG  # noqa: F401  Airflow only parses files that mention "DAG"; the factory builds them

from transform_strategy import TransformStrategy
from marts_dag_factory import MartsDagFactory
from spark_job_strategy import SparkJobStrategy
from core_dag_factory import DagFactory
from pipeline_config import PIPELINE_CONFIGS
from config.loader import ConfigLoader


for pipeline_config in PIPELINE_CONFIGS:
    globals()[pipeline_config.dag_id] = DagFactory(
        pipeline_config=pipeline_config,
        spark_job_strategy=SparkJobStrategy.build_strategy(config=ConfigLoader()),
        transform_strategy=TransformStrategy.build_strategy(config=ConfigLoader()),
    ).build()

gpu_arbitrage__marts = MartsDagFactory(
    pipeline_configs=PIPELINE_CONFIGS,
    transform_strategy=TransformStrategy.build_strategy(config=ConfigLoader()),
).build()
