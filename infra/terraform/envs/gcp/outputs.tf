output "workload_identity_provider_name" {
  description = "Value for GCP_WORKLOAD_IDENTITY_PROVIDER_NAME secret in GitHub"
  value       = google_iam_workload_identity_pool_provider.github_workload_identity_pool_provider.name
}

output "github_actions_service_account_email" {
  description = "Value for GCP_SERVICE_ACCOUNT_EMAIL secret in GitHub"
  value       = google_service_account.github_actions_sa.email
}

output "dataproc_service_account_email" {
  description = "Value for gcp.dataproc.service_account_email in settings.yaml"
  value       = google_service_account.dataproc_sa.email
}

output "artifact_registry_url" {
  description = "Base URL for the Artifact Registry repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository_name}"
}

output "composer_dags_bucket" {
  description = "GCS bucket to upload DAGs and src/ into"
  value       = google_composer_environment.composer.config[0].dag_gcs_prefix
}

output "composer_airflow_uri" {
  description = "Airflow web UI URL"
  value       = google_composer_environment.composer.config[0].airflow_uri
}
