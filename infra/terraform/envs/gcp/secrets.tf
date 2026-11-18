# The API keys the python job's ingest needs. The values go in write-only, so they never reach Terraform state (ADR-028).
locals {
  api_key_secrets = {
    VASTAI_API_KEY        = "vastai-api-key"
    EXCHANGE_RATE_API_KEY = "exchange-rate-api-key"
  }
  api_key_values = {
    VASTAI_API_KEY        = var.vastai_api_key
    EXCHANGE_RATE_API_KEY = var.exchange_rate_api_key
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

# A write-only value has no state to diff, so Terraform pushes a new value only when api_keys_version changes
resource "google_secret_manager_secret_version" "api_keys" {
  for_each               = google_secret_manager_secret.api_keys
  secret                 = each.value.id
  secret_data_wo         = local.api_key_values[each.key]
  secret_data_wo_version = var.api_keys_version
}
