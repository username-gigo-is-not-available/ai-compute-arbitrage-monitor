# The API keys the python job's ingest needs. Terraform creates the empty secrets and the access rule; the values are
# added by hand (gcloud secrets versions add), so they are in neither Terraform state nor the job's config (ADR-028).
locals {
  api_key_secrets = {
    VASTAI_API_KEY        = "vastai-api-key"
    EXCHANGE_RATE_API_KEY = "exchange-rate-api-key"
  }
}

resource "google_secret_manager_secret" "api_keys" {
  for_each  = local.api_key_secrets
  secret_id = each.value

  replication {
    auto {}
  }

  depends_on = [google_project_service.enabled_services["secret_manager"]]
}

resource "google_secret_manager_secret_iam_member" "python_reads_api_keys" {
  for_each  = google_secret_manager_secret.api_keys
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.python_cloud_run_sa.email}"
}
