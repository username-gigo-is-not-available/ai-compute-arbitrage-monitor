# ── Cloud Run Job ──────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_job" "dbt_transform" {
  name                = var.dbt_cloud_run_job_name
  location            = var.region
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

# ── Python Cloud Run Job: setup and ingest (ADR-028) ─────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_job" "python" {
  name                = var.python_cloud_run_job_name
  location            = var.region
  deletion_protection = false

  depends_on = [
    google_project_service.enabled_services["run"],
    google_secret_manager_secret_iam_member.python_reads_api_keys,
  ]

  template {
    template {
      service_account = google_service_account.python_cloud_run_sa.email

      # The Vast.ai census takes minutes, with retries and Retry-After waits
      timeout     = "1800s"
      max_retries = 0

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository_name}/${var.python_cloud_run_image_name}:latest"

        # No default args: each run passes "lakehouse.main <dataset>" or "ingest.main <dataset> <scheduled_at>"

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }

        dynamic "env" {
          for_each = google_secret_manager_secret.api_keys
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = env.value.secret_id
                version = "latest"
              }
            }
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
    }
  }
}
