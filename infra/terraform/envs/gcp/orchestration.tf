resource "google_composer_environment" "composer" {
  # Composer runs on the VPC, so it needs the network too
  count  = var.enable_composer && var.enable_network ? 1 : 0
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

      # Only what parsing the DAG files needs beyond Airflow's own dependencies; the pipeline runs in the
      # python Cloud Run job, so its libraries are not installed here (ADR-028)
      pypi_packages = {
        python-dotenv = "==1.2.4"
        pyyaml        = "==6.0.3"
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
      network         = google_compute_network.vpc[0].id
      subnetwork      = google_compute_subnetwork.subnet[0].id
      service_account = google_service_account.composer_sa.email
    }
  }
}
