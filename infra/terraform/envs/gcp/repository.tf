resource "google_artifact_registry_repository" "image_repository" {
  repository_id = var.ar_repository_name
  location      = var.region
  format        = "DOCKER"
  description   = "Dataproc Serverless custom container images"

  depends_on = [google_project_service.enabled_services["artifact_registry"]]
}
