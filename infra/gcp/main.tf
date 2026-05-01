terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.18.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── APIs ───────────────────────────────────────────────────────────────────────

locals {
  services = {
    compute           = "compute.googleapis.com"
    storage           = "storage.googleapis.com"
    bigquery          = "bigquery.googleapis.com"
    iam               = "iam.googleapis.com"
    resourcemanager   = "cloudresourcemanager.googleapis.com"
    dataproc          = "dataproc.googleapis.com"
    artifact_registry = "artifactregistry.googleapis.com"
  }
}

resource "google_project_service" "enabled_services" {
  for_each           = local.services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# ── Storage ────────────────────────────────────────────────────────────────────

resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = var.buckets_force_destroy

  depends_on = [google_project_service.enabled_services["storage"]]
}

# ── Networking ─────────────────────────────────────────────────────────────────

resource "google_compute_network" "vpc" {
  name                    = var.vpc_name
  auto_create_subnetworks = false

  depends_on = [google_project_service.enabled_services["compute"]]
}

resource "google_compute_subnetwork" "subnet" {
  name                     = var.subnet_name
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true

  depends_on = [google_project_service.enabled_services["compute"]]
}

resource "google_compute_firewall" "allow_internal" {
  name    = "allow-internal-dataproc"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }
  source_ranges = [var.subnet_cidr]

  depends_on = [google_project_service.enabled_services["compute"]]
}

resource "google_compute_router" "router" {
  name    = var.compute_router_name
  region  = var.region
  network = google_compute_network.vpc.id

  depends_on = [google_project_service.enabled_services["compute"]]
}

resource "google_compute_router_nat" "nat" {
  name                               = var.router_nat_name
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  depends_on = [google_project_service.enabled_services["compute"]]
}

# ── BigQuery ───────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location

  depends_on = [google_project_service.enabled_services["bigquery"]]
}

# ── Artifact Registry ──────────────────────────────────────────────────────────

resource "google_artifact_registry_repository" "image_repository" {
  repository_id = var.ar_repository_name
  location      = var.region
  format        = "DOCKER"
  description   = "Dataproc Serverless custom container images"

  depends_on = [google_project_service.enabled_services["artifact_registry"]]
}

# ── Service Account & IAM ──────────────────────────────────────────────────────

resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-sa"
  display_name = "Dataproc Serverless Service Account"

  depends_on = [google_project_service.enabled_services["iam"]]
}

resource "google_project_iam_member" "dataproc_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/dataproc.admin",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/artifactregistry.reader",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"

  depends_on = [google_project_service.enabled_services["resourcemanager"]]
}
