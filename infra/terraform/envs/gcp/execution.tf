# ── Cloud Run Job ──────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_job" "dbt_transform" {
  name     = var.dbt_cloud_run_job_name
  location = var.region
  deletion_protection = false

  depends_on = [
    google_project_service.enabled_services["composer"],
    google_service_account.dbt_cloud_run_sa,
  ]

  template {
    template {
      service_account = google_service_account.dbt_cloud_run_sa.email

      timeout = "1800s"

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository_name}/${var.dbt_cloud_run_image_name}:latest"

        args = ["run"]

        # Required by src/transform/profiles.yml (ADR-013, ADR-018: BQ_LOCATION has no default).
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "BQ_DATASET_NAME"
          value = var.bq_dataset_name
        }
        env {
          name  = "BQ_LOCATION"
          value = var.location
        }

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }
      }
    }
  }
}