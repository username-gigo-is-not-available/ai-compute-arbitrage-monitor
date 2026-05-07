output "gcs_bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.bucket.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.dataset.dataset_id
}
