import argparse
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pyiceberg.catalog import Catalog

from common.enums import DatasetType, DataStageType
from config.lakehouse import GCPLakehouseConfig
from config.loader import ConfigLoader

load_dotenv()
logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def load_settings() -> dict:
    path = Path(os.getenv("SETTINGS_PATH", "config/settings.yaml"))
    with open(path) as f:
        return yaml.safe_load(f)


def _setup_logging(log_config: dict | None = None) -> None:
    log_config = log_config or {}
    logging.basicConfig(
        level=log_config.get("level", "INFO"),
        format=log_config.get("format", _LOG_FORMAT),
        force=True,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Purge pipeline data (Bronze/Silver Iceberg tables and their GCS files, Gold/BigQuery dataset) for local dev reset.",
        epilog=(
            "Destructive and irreversible. No flags purges nothing; without --confirm the script "
            "asks interactively before deleting. GCP identity comes from the environment "
            "(GCS_BUCKET_NAME, GCP_PROJECT_ID, BQ_DATASET_NAME) per ADR-013."
        ),
    )
    stages = parser.add_mutually_exclusive_group(required=True)
    stages.add_argument(
        "--bronze",
        action="store_true",
        help="Drop all Iceberg tables in bronze_sources/bronze_seeds and delete gs://{bucket}/bronze_*/**",
    )
    stages.add_argument(
        "--silver",
        action="store_true",
        help="Drop all Iceberg tables in silver_sources/silver_seeds and delete gs://{bucket}/silver_*/**",
    )
    stages.add_argument(
        "--gold",
        action="store_true",
        help="Drop the BigQuery dataset (Gold layer); the next `dbt run` recreates it and all tables",
    )
    stages.add_argument("--all", action="store_true", help="Bronze + silver + gold")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip the interactive y/n confirmation and delete immediately",
    )
    return parser.parse_args(argv)


def stage_namespaces(stage: DataStageType) -> list[str]:
    return [GCPLakehouseConfig.stage_namespace(stage, dataset_type) for dataset_type in DatasetType]


def open_catalog() -> Catalog:
    # Module-level seam: tests replace it with a fake catalog.
    return ConfigLoader().get_lakehouse().open_catalog()


def purge_iceberg_stage(catalog: Catalog, bucket: str, stage: DataStageType) -> None:
    """Drop the stage's Iceberg tables, then delete each namespace's GCS prefix.

    drop_table only removes the catalog entry; the data files stay in GCS. Deleting the
    whole namespace prefix also removes orphans left by tables dropped earlier. Tables
    are dropped first so a failed file delete leaves orphans, never dangling metadata.
    """
    gcloud_bin = shutil.which("gcloud") or "gcloud"
    for namespace in stage_namespaces(stage):
        for identifier in catalog.list_tables(namespace):
            logger.info(f"Dropping Iceberg table {'.'.join(identifier)}")
            catalog.drop_table(identifier)
        target: str = f"gs://{bucket}/{namespace}/**"
        logger.info(f"Purging GCS {stage.value} data - {target}")
        try:
            subprocess.run([gcloud_bin, "storage", "rm", "-r", target], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"gcloud failed for {target} - {e}")
            sys.exit(1)


def purge_bigquery(project: str, dataset: str) -> None:
    target: str = f"{project}:{dataset}"
    bq_bin = shutil.which("bq") or "bq"
    logger.info(f"Purging BigQuery dataset (Gold layer) - {target} (tables are dropped with it)")
    try:
        subprocess.run([bq_bin, "rm", "-r", "-f", "-d", target], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"bq failed for {target} - {e}")
        sys.exit(1)
    logger.info("BigQuery dataset dropped. Recreate tables with `make dbt-run` (dbt run).")


def selected_stages(args: argparse.Namespace) -> list[DataStageType]:
    if args.all:
        return [DataStageType.BRONZE, DataStageType.SILVER, DataStageType.GOLD]
    if args.gold:
        return [DataStageType.GOLD]
    if args.bronze:
        return [DataStageType.BRONZE]
    return [DataStageType.SILVER]


def log_plan(stages: list[DataStageType], bucket: str, project: str, dataset: str) -> None:
    logger.info("The following destructive actions will be executed:")
    for stage in stages:
        if stage == DataStageType.GOLD:
            logger.info(f"  - bq rm -r -f -d {project}:{dataset}")
        else:
            for namespace in stage_namespaces(stage):
                logger.info(
                    f"  - drop all Iceberg tables in {namespace}; gcloud storage rm -r gs://{bucket}/{namespace}/**"
                )


def confirm_or_exit() -> None:
    try:
        answer: str = input("Type 'yes' to confirm (anything else aborts): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        answer = ""
    if answer != "yes":
        logger.error("Aborted - nothing was deleted.")
        sys.exit(1)


def main() -> None:
    args: argparse.Namespace = parse_args()

    try:
        cfg: dict = load_settings()
    except FileNotFoundError as e:
        logger.error(f"Could not load settings - {e}")
        sys.exit(1)
    _setup_logging(cfg.get("logging"))

    bucket: str = os.environ["GCS_BUCKET_NAME"].strip("/")
    project: str = os.environ["GCP_PROJECT_ID"]
    dataset: str = os.environ["BQ_DATASET_NAME"]

    stages = selected_stages(args)
    log_plan(stages, bucket, project, dataset)
    if not args.confirm:
        confirm_or_exit()

    catalog: Catalog | None = None  # opened only when a Bronze or Silver stage is purged
    for stage in stages:
        if stage == DataStageType.GOLD:
            purge_bigquery(project, dataset)
        else:
            catalog = catalog or open_catalog()
            purge_iceberg_stage(catalog, bucket, stage)

    logger.info("Purge complete.")


def run() -> None:
    main()


if __name__ == "__main__":
    run()
