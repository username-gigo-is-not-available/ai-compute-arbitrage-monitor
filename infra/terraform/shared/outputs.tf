output "gcs_bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.bucket.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.dataset.dataset_id
}

output "lakehouse_catalog_id" {
  description = "BigLake Iceberg REST catalog ID"
  value       = google_biglake_iceberg_catalog.lakehouse.name
}
