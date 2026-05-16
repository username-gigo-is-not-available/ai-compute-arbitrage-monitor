variable "project_id" {}
variable "region" { default = "europe-west3" }
variable "location" { default = "europe-west3" }

# Networking
variable "vpc_name" {}
variable "subnet_name" {}
variable "subnet_cidr" { default = "10.0.0.0/24" }
variable "compute_router_name" {}
variable "router_nat_name" {}

# Artifact Registry
variable "ar_repository_name" {}

# Cloud Composer
variable "cc_environment_name" {}

# Storage
variable "gcs_bucket_name" {}

# GitHub Workload Identity Federation
variable "gh_wif_pool_id" {}
variable "gh_wif_pool_provider_id" {}
variable "gh_repository_uri" {}
