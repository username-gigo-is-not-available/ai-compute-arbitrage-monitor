locals {
  services = {
    storage         = "storage.googleapis.com"
    bigquery        = "bigquery.googleapis.com"
    iam             = "iam.googleapis.com"
    resourcemanager = "cloudresourcemanager.googleapis.com"
  }
}

resource "google_project_service" "enabled_services" {
  for_each           = local.services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
