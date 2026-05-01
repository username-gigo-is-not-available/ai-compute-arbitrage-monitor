import logging
import sys
import time
import zipfile
from pathlib import Path

from google.cloud import storage
from google.cloud.dataproc_v1 import BatchControllerClient
from google.cloud.dataproc_v1.types import (
    Batch,
    PySparkBatch,
    RuntimeConfig,
    EnvironmentConfig,
    ExecutionConfig,
)

from config.cluster import GCPClusterConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig


class JobBootstrapper:
    def __init__(
        self,
        storage_config: GCPStorageConfig,
        cluster_config: GCPClusterConfig,
    ):
        self.storage_config = storage_config
        self.cluster_config = cluster_config
        self.project_root = Path(__file__).parent.parent.parent
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def entrypoints(self) -> dict[str, str]:
        bucket = self.storage_config.bucket_name
        entrypoints_path: str = self.cluster_config.entrypoints_path
        return {
            "electricity_tariff_prices":    f"gs://{bucket}/{entrypoints_path}/electricity_tariff_prices.py",
            "electricity_tariffs_schedule": f"gs://{bucket}/{entrypoints_path}/electricity_tariffs_schedule.py",
            "compute_offers":               f"gs://{bucket}/{entrypoints_path}/compute_offers.py",
            "exchange_rates":               f"gs://{bucket}/{entrypoints_path}/exchange_rates.py",
        }

    def _zip_src(self) -> Path:
        zip_path = self.project_root / "src.zip"
        src_dir = self.project_root / "src"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for package in self.cluster_config.runtime_packages:
                package_dir = src_dir / package
                for file in package_dir.rglob("*.py"):
                    arcname = file.relative_to(src_dir).as_posix()
                    zf.write(file, arcname)

        self.logger.info(f"Zipped runtime src/ → {zip_path}")
        return zip_path

    def upload_to_gcs(self) -> None:
        client = storage.Client()
        bucket_name: str = self.storage_config.bucket_name
        bucket = client.bucket(bucket_name)

        settings = self.project_root / "config" / "settings.yaml"
        if settings.exists():
            blob = bucket.blob("config/settings.yaml")
            blob.upload_from_filename(str(settings))
            self.logger.info(f"Uploaded settings → gs://{bucket_name}/config/settings.yaml")
        else:
            self.logger.warning("config/settings.yaml not found, skipping.")

        entrypoints_path = self.project_root / self.cluster_config.entrypoints_path
        for file in entrypoints_path.glob("*.py"):
            gcs_key = f"{self.cluster_config.entrypoints_path}/{file.name}"
            bucket.blob(gcs_key).upload_from_filename(str(file))
            self.logger.info(f"Uploaded pipeline → gs://{bucket_name}/{gcs_key}")

        zip_path = self._zip_src()
        bucket.blob("src.zip").upload_from_filename(str(zip_path))
        self.logger.info(f"Uploaded src.zip → gs://{bucket_name}/src.zip")
        zip_path.unlink()

    def submit_batch(self, entrypoint: str) -> str:
        if entrypoint not in self.entrypoints:
            self.logger.error(f"Unknown job '{entrypoint}'. Available: {list(self.entrypoints)}")
            sys.exit(1)

        client = BatchControllerClient(
            client_options={
                "api_endpoint": f"{self.cluster_config.region_name}-dataproc.googleapis.com:443"
            }
        )

        settings_uri = f"gs://{self.storage_config.bucket_name}/config/settings.yaml"

        batch = Batch(
            pyspark_batch=PySparkBatch(
                main_python_file_uri=self.entrypoints[entrypoint],
            ),
            runtime_config=RuntimeConfig(
                container_image=self.cluster_config.image_tag,
                properties={
                    "spark.sql.parquet.compression.codec": "snappy",
                    "spark.executorEnv.SETTINGS_PATH": settings_uri,
                    "spark.yarn.appMasterEnv.SETTINGS_PATH": settings_uri,
                },
            ),
            environment_config=EnvironmentConfig(
                execution_config=ExecutionConfig(
                    subnetwork_uri=self.cluster_config.subnetwork_name,
                    service_account=self.cluster_config.service_account_email,
                )
            ),
        )

        batch_id = f"{entrypoint.replace("_", "-")}-{{{{ ts_nodash | lower }}}}"


        client.create_batch(
            parent=f"projects/{self.cluster_config.project_id}/locations/{self.cluster_config.region_name}",
            batch=batch,
            batch_id=batch_id,
        )

        self.logger.info(f"Submitted batch: {batch_id}")
        return batch_id

    def wait_for_batch(self, batch_id: str) -> None:
        self.logger.info(f"Waiting for batch {batch_id} to complete...")

        client = BatchControllerClient(
            client_options={
                "api_endpoint": f"{self.cluster_config.region_name}-dataproc.googleapis.com:443"
            }
        )

        batch_name = (
            f"projects/{self.cluster_config.project_id}"
            f"/locations/{self.cluster_config.region_name}"
            f"/batches/{batch_id}"
        )

        while True:
            batch = client.get_batch(name=batch_name)
            state = batch.state

            if state == Batch.State.SUCCEEDED:
                self.logger.info("Batch completed successfully.")
                break
            elif state == Batch.State.FAILED:
                self.logger.error(f"Batch failed: {batch.state_message}")
                sys.exit(1)
            elif state == Batch.State.CANCELLED:
                self.logger.warning("Batch was cancelled.")
                sys.exit(1)

            self.logger.info(f"Current state: {state.name}...")
            time.sleep(15)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python jobs.py [job_name]")
        print("Available jobs: electricity_tariff_prices, electricity_tariffs_schedule, compute_offers, exchange_rates")
        sys.exit(1)

    entrypoint = sys.argv[1]

    loader = ConfigLoader()
    storage_config = loader.get_storage()
    cluster_config = loader.get_cluster()

    bootstrapper = JobBootstrapper(
        storage_config=storage_config,
        cluster_config=cluster_config,
    )

    bootstrapper.upload_to_gcs()
    batch_id = bootstrapper.submit_batch(entrypoint)
    bootstrapper.wait_for_batch(batch_id)
