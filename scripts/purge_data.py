import argparse
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
import yaml

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from common.enums import DataStageType  # noqa: E402

load_dotenv()

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
        description="Purge pipeline data (Bronze/Silver GCS objects, Gold/BigQuery dataset) for local dev reset.",
        epilog=(
            "Destructive and irreversible. No flags purges nothing; without --confirm the script "
            "asks interactively before deleting. GCP identity comes from the environment "
            "(GCS_BUCKET_NAME, GCP_PROJECT_ID, BQ_DATASET_NAME) per ADR-013."
        ),
    )
    stages = parser.add_mutually_exclusive_group(required=True)
    stages.add_argument("--bronze", action="store_true", help="Delete all objects under gs://{bucket}/bronze/**")
    stages.add_argument("--silver", action="store_true", help="Delete all objects under gs://{bucket}/silver/**")
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


def purge_gcs_stage(bucket: str, stage: DataStageType) -> None:
    target: str = f"gs://{bucket}/{stage.value}/**"
    gcloud_bin = shutil.which("gcloud") or "gcloud"
    logging.info(f"Purging GCS {stage.value} data - {target}")
    try:
        subprocess.run([gcloud_bin, "storage", "rm", "-r", target], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"gcloud failed for {target} - {e}")
        sys.exit(1)


def purge_bigquery(project: str, dataset: str) -> None:
    target: str = f"{project}:{dataset}"
    bq_bin = shutil.which("bq") or "bq"
    logging.info(f"Purging BigQuery dataset (Gold layer) - {target} (tables are dropped with it)")
    try:
        subprocess.run([bq_bin, "rm", "-r", "-f", "-d", target], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"bq failed for {target} - {e}")
        sys.exit(1)
    logging.info("BigQuery dataset dropped. Recreate tables with `make dbt-run` (dbt run).")


def main() -> None:
    args: argparse.Namespace = parse_args()

    try:
        cfg: dict = load_settings()
    except FileNotFoundError as e:
        logging.error(f"Could not load settings - {e}")
        sys.exit(1)
    _setup_logging(cfg.get("logging"))

    bucket: str = os.environ["GCS_BUCKET_NAME"].strip("/")
    project: str = os.environ["GCP_PROJECT_ID"]
    dataset: str = os.environ["BQ_DATASET_NAME"]

    if args.all:
        stages: list[DataStageType] = [
            DataStageType.BRONZE,
            DataStageType.SILVER,
            DataStageType.GOLD,
        ]
    elif args.gold:
        stages = [DataStageType.GOLD]
    elif args.bronze:
        stages = [DataStageType.BRONZE]
    else:
        stages = [DataStageType.SILVER]

    logging.info("The following destructive actions will be executed:")
    for stage in stages:
        if stage == DataStageType.GOLD:
            logging.info(f"  - bq rm -r -f -d {project}:{dataset}")
        else:
            logging.info(f"  - gcloud storage rm -r gs://{bucket}/{stage.value}/**")

    if not args.confirm:
        try:
            answer: str = input("Type 'yes' to confirm (anything else aborts): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = ""
        if answer != "yes":
            logging.error("Aborted - nothing was deleted.")
            sys.exit(1)

    for stage in stages:
        if stage == DataStageType.GOLD:
            purge_bigquery(project, dataset)
        else:
            purge_gcs_stage(bucket, stage)

    logging.info("Purge complete.")


def run() -> None:
    main()


if __name__ == "__main__":
    run()