# ── Cloud Run Job ──────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_job" "dbt_transform" {
  name     = var.dbt_cloud_run_job_name
  location = var.region

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