resource "google_biglake_iceberg_catalog" "lakehouse" {
  name             = var.lakehouse_catalog_id
  catalog_type     = "CATALOG_TYPE_BIGLAKE"
  default_location = "gs://${var.gcs_bucket_name}"
  credential_mode  = "CREDENTIAL_MODE_END_USER"
  primary_location = var.region
  project          = var.project_id

  depends_on = [google_project_service.enabled_services["biglake"]]
}
