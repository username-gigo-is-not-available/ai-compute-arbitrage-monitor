resource "google_composer_environment" "composer" {
  name   = var.cc_environment_name
  region = var.region

  depends_on = [
    google_project_service.enabled_services["composer"],
    google_compute_subnetwork.subnet,
    google_service_account.composer_sa,
  ]

  config {
    software_config {
      image_version = "composer-3-airflow-3.1.7"

      pypi_packages = {
        pydantic       = ">=2.13.3"
        pyyaml         = ">=6.0"
        aiohttp        = ">=3.13.4"
        requests       = ">=2.33.0"
        beautifulsoup4 = ">=4.14.3"
        pyarrow        = ">=24.0.0"
        python-dotenv  = ">=1.2.2"
        certifi        = ""
        google-cloud-dataproc = ">=5.27.0"
      }

      env_variables = {
        SETTINGS_PATH   = "gs://${var.gcs_bucket_name}/config/settings.yaml"
        DBT_PROJECT_DIR = "/home/airflow/gcs/dags/transform"
        DBT_PROFILES_DIR = "/home/airflow/gcs/dags/transform"
        DBT_TARGET_DIR  = "/tmp/dbt_target"
        PYTHONPATH       = "/home/airflow/gcs/dags"
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      network         = google_compute_network.vpc.id
      subnetwork      = google_compute_subnetwork.subnet.id
      service_account = google_service_account.dataproc_sa.email
    }
  }
}

