resource "google_composer_environment" "composer" {
  name   = var.cc_environment_name
  region = var.region

  depends_on = [
    google_project_service.enabled_services["composer"],
    google_compute_subnetwork.subnet,
    google_service_account.composer_sa,
    google_service_account_iam_member.composer_impersonates_dataproc,
  ]

  config {
    software_config {
      image_version = "composer-3-airflow-3.1.7"

      airflow_config_overrides = merge(
        {
          "core-dags_are_paused_at_creation" = "true"
          "core-max_active_runs_per_dag"     = "1"
        },
        var.cc_image_tag != null ? {
          "core-custom_image" = var.cc_image_tag
        } : {}
      )


      env_variables = {
        SETTINGS_PATH    = "gs://${var.gcs_bucket_name}/config/settings.yaml"
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
