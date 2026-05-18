resource "google_composer_environment" "composer" {
  name   = var.cc_environment_name
  region = var.region

  depends_on = [
    google_project_service.enabled_services["composer"],
    google_compute_subnetwork.subnet,
    google_service_account.composer_sa
  ]

  storage_config {
    bucket = var.gcs_bucket_name
  }

  config {
    software_config {
      image_version = "composer-3-airflow-3.1.7"

      airflow_config_overrides = {
        "core-dags_are_paused_at_creation" = "true"
        "core-max_active_runs_per_dag"     = "1"
      }

      pypi_packages = {
        beautifulsoup4 = "==4.14.3"
      }

      env_variables = {
        SETTINGS_PATH    = "/home/airflow/gcs/dags/config/settings.yaml"
        DBT_PROJECT_DIR  = "/home/airflow/gcs/dags/transform"
        DBT_PROFILES_DIR = "/home/airflow/gcs/dags/transform"
        DBT_TARGET_DIR   = "/tmp/dbt_target"
        PYTHONPATH       = "/home/airflow/gcs/dags"
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"


    node_config {
      network         = google_compute_network.vpc.id
      subnetwork      = google_compute_subnetwork.subnet.id
      service_account = google_service_account.composer_sa.email
    }
  }
}
