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

# ── Storage ────────────────────────────────────────────────────────────────────

resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = var.buckets_force_destroy
}

resource "google_storage_bucket" "cluster_staging" {
  name          = "${var.gcs_bucket_name}-dataproc-staging"
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = var.staging_bucket_retention_days }
  }
}

# ── Networking ─────────────────────────────────────────────────────────────────

resource "google_compute_network" "vpc" {
  name                    = var.vpc_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name                     = var.subnet_name
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
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
}

resource "google_compute_router" "router" {
  name    = var.compute_router_name
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = var.router_nat_name
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# ── BigQuery ───────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# ── Init script ────────────────────────────────────────────────────────────────

resource "google_storage_bucket_object" "pyproject" {
  name   = "env/pyproject.toml"
  bucket = google_storage_bucket.bucket.name
  source = "${path.module}/../../pyproject.toml"
}

resource "google_storage_bucket_object" "uv_lock" {
  name   = "env/uv.lock"
  bucket = google_storage_bucket.bucket.name
  source = "${path.module}/../../uv.lock"
}

resource "google_storage_bucket_object" "init_script" {
  name    = "init-dependencies.sh"
  bucket  = google_storage_bucket.bucket.name
  content = replace(file("${path.module}/init-dependencies.sh"), "\r\n", "\n")
}
# ── Service account ────────────────────────────────────────────────────────────

resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-sa"
  display_name = "Dataproc Service Account"
}

resource "google_project_iam_member" "dataproc_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser"
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# ── Dataproc cluster ───────────────────────────────────────────────────────────

resource "google_dataproc_cluster" "gpu_cluster" {
  name   = var.dataproc_cluster_name
  region = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.cluster_staging.name

    software_config {
      image_version = var.dataproc_image_version

    }

    gce_cluster_config {
      subnetwork       = google_compute_subnetwork.subnet.id
      service_account  = google_service_account.dataproc_sa.email
      internal_ip_only = true

      metadata = {
        "project-bucket" = google_storage_bucket.bucket.name
      }
    }

    master_config {
      num_instances = var.dataproc_master_node_number_of_instances
      machine_type  = var.dataproc_master_node_machine_type
      disk_config {
        boot_disk_size_gb = var.dataproc_master_node_disk_size_gb
      }
    }

    worker_config {
      num_instances = var.dataproc_worker_node_number_of_instances
      machine_type  = var.dataproc_worker_node_machine_type
      disk_config {
        boot_disk_size_gb = var.dataproc_worker_node_disk_size_gb
      }
    }
    initialization_action {
      script      = "gs://${google_storage_bucket.bucket.name}/init-dependencies.sh"
      timeout_sec = var.init_script_timeout_sec
    }
  }
}
