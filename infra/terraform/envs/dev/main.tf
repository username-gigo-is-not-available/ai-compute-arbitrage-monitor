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

# Future local-only resources (e.g. local test datasets) go here.
