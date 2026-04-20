import logging
import sys
import time
import zipfile
from pathlib import Path

from google.cloud import storage
from google.cloud.dataproc_v1 import JobControllerClient
from google.cloud.dataproc_v1.types import Job, PySparkJob, JobPlacement
from google.cloud.dataproc_v1.types.jobs import JobStatus

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
        return {
            "electricity_tariff_prices":    f"gs://{bucket}/src/refine/pipelines/electricity_tariff_prices.py",
            "electricity_tariffs_schedule": f"gs://{bucket}/src/refine/pipelines/electricity_tariffs_schedule.py",
            "compute_offers":               f"gs://{bucket}/src/refine/pipelines/compute_offers.py",
            "exchange_rates":               f"gs://{bucket}/src/refine/pipelines/exchange_rates.py",
        }

    def _zip_src(self) -> Path:
        zip_path = self.project_root / "src.zip"
        src_dir = self.project_root / "src"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file in src_dir.rglob("*.py"):
                if "__pycache__" in file.parts or ".egg-info" in str(file):
                    continue
                arcname = file.relative_to(src_dir).as_posix()
                zf.write(file, arcname)

        self.logger.info(f"Zipped src/ → {zip_path}")
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

        pipelines_dir = self.project_root / "src" / "refine" / "pipelines"
        for file in pipelines_dir.glob("*.py"):
            gcs_key = f"src/refine/pipelines/{file.name}"
            bucket.blob(gcs_key).upload_from_filename(str(file))
            self.logger.info(f"Uploaded pipeline → gs://{bucket_name}/{gcs_key}")

        zip_path = self._zip_src()
        bucket.blob("src.zip").upload_from_filename(str(zip_path))
        self.logger.info(f"Uploaded src.zip → gs://{bucket_name}/src.zip")
        zip_path.unlink()

    def submit_job(self, job_name: str) -> str:
        if job_name not in self.entrypoints:
            self.logger.error(f"Unknown job '{job_name}'. Available: {list(self.entrypoints)}")
            sys.exit(1)

        client = JobControllerClient(
            client_options={
                "api_endpoint": f"{self.cluster_config.region_name}-dataproc.googleapis.com:443"
            }
        )

        job = Job(
            placement=JobPlacement(cluster_name=self.cluster_config.cluster_name),
            pyspark_job=PySparkJob(
                main_python_file_uri=self.entrypoints[job_name],
                python_file_uris=[
                    f"gs://{self.storage_config.bucket_name}/src.zip",
                ],
                file_uris=[
                    f"gs://{self.storage_config.bucket_name}/config/settings.yaml"
                ],
                properties={

                    "spark.yarn.appMasterEnv.SETTINGS_PATH": f"gs://{self.storage_config.bucket_name}/config/settings.yaml",
                    "spark.executorEnv.SETTINGS_PATH": f"gs://{self.storage_config.bucket_name}/config/settings.yaml",
                    "spark.submit.deployMode": "client",
                    "spark.pyspark.python": "/opt/gpu_arbitrage/.venv/bin/python",
                    "spark.pyspark.driver.python": "/opt/gpu_arbitrage/.venv/bin/python",
                    "spark.submit.pyFiles": f"gs://{self.storage_config.bucket_name}/src.zip"
                },
            ),
        )

        response = client.submit_job(
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region_name,
            job=job,
        )
        job_id = response.reference.job_id
        self.logger.info(f"Submitted job: {job_id}")
        return job_id

    def wait_for_job(self, job_id: str) -> None:
        self.logger.info("Waiting for job to complete...")

        client = JobControllerClient(
            client_options={
                "api_endpoint": f"{self.cluster_config.region_name}-dataproc.googleapis.com:443"
            }
        )

        while True:
            job = client.get_job(
                project_id=self.cluster_config.project_id,
                region=self.cluster_config.region_name,
                job_id=job_id,
            )
            status = job.status.state

            if status == JobStatus.State.DONE:
                self.logger.info("Job completed successfully!")
                break
            elif status == JobStatus.State.ERROR:
                self.logger.error(f"Job failed: {job.status.details}")
                sys.exit(1)
            elif status == JobStatus.State.CANCELLED:
                self.logger.warning("Job was cancelled.")
                sys.exit(1)

            self.logger.info(f"Current status: {status.name}...")
            time.sleep(10)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python jobs.py [job_name]")
        print("Available jobs: electricity_tariff_prices, electricity_tariffs_schedule, compute_offers, exchange_rates")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    job_name = sys.argv[1]

    loader = ConfigLoader()
    storage_config = loader.get_storage()
    cluster_config = loader.get_cluster()

    bootstrapper = JobBootstrapper(
        storage_config=storage_config,
        cluster_config=cluster_config,
    )

    bootstrapper.upload_to_gcs()
    job_id = bootstrapper.submit_job(job_name)
    bootstrapper.wait_for_job(job_id)
