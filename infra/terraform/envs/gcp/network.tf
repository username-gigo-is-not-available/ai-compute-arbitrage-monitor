resource "google_compute_network" "vpc" {
  count = var.enable_network ? 1 : 0

  name                    = var.vpc_name
  auto_create_subnetworks = false

  depends_on = [google_project_service.enabled_services["compute"]]
}

resource "google_compute_subnetwork" "subnet" {
  count = var.enable_network ? 1 : 0

  name                     = var.subnet_name
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc[0].id
  private_ip_google_access = true

  depends_on = [google_project_service.enabled_services["compute"]]
}

resource "google_compute_firewall" "allow_internal" {
  count = var.enable_network ? 1 : 0

  name    = "allow-internal-dataproc"
  network = google_compute_network.vpc[0].name

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
  count = var.enable_network ? 1 : 0

  name    = var.compute_router_name
  region  = var.region
  network = google_compute_network.vpc[0].id

  depends_on = [google_project_service.enabled_services["compute"]]
}

resource "google_compute_router_nat" "nat" {
  count = var.enable_network ? 1 : 0

  name                               = var.router_nat_name
  router                             = google_compute_router.router[0].name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  depends_on = [google_project_service.enabled_services["compute"]]
}
