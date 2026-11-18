variable "project_id" {}
variable "region" { default = "europe-west3" }
variable "location" { default = "europe-west3" }

# Off by default: Composer bills ~$400+/month even when idle, and the VPC + Cloud NAT exist only for Dataproc and
# Composer. Cloud Run jobs need neither.
variable "enable_composer" { default = false }
variable "enable_network" { default = false }

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
variable "bq_dataset_name" {}

# GitHub Workload Identity Federation
variable "gh_wif_pool_id" {}
variable "gh_wif_pool_provider_id" {}
variable "gh_repository_uri" {}

# dbt Cloud Run
variable "dbt_cloud_run_job_name" {}
variable "dbt_cloud_run_image_name" {}

# Python Cloud Run: setup and ingest, one job with args per run (ADR-028)
variable "python_cloud_run_job_name" {}
variable "python_cloud_run_image_name" {}

# API keys: set TF_VAR_vastai_api_key and TF_VAR_exchange_rate_api_key from .env; ephemeral keeps them out of plan and state
variable "vastai_api_key" {
  type      = string
  sensitive = true
  ephemeral = true
}
variable "exchange_rate_api_key" {
  type      = string
  sensitive = true
  ephemeral = true
}
variable "api_keys_version" {
  type    = number
  default = 1
}
