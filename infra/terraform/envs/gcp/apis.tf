locals {
  services = {
    compute               = "compute.googleapis.com"
    dataproc              = "dataproc.googleapis.com"
    artifact_registry     = "artifactregistry.googleapis.com"
    container_file_system = "containerfilesystem.googleapis.com"
  }
}

resource "google_project_service" "enabled_services" {
  for_each           = local.services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
