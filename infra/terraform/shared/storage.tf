resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = var.buckets_force_destroy
  uniform_bucket_level_access = true
  depends_on = [google_project_service.enabled_services["storage"]]
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location

  depends_on = [google_project_service.enabled_services["bigquery"]]
}
