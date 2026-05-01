variable "project_id" {}
variable "region" { default = "europe-west3" }
variable "location" { default = "EU" }
variable "vpc_name" {}
variable "subnet_name" {}
variable "compute_router_name" {}
variable "router_nat_name" {}
variable "bq_dataset_name" {}
variable "gcs_bucket_name" {}
variable "ar_repository_name" {}
variable "gh_wif_pool_id" {}
variable "gh_wif_pool_provider_id" {}
variable "gh_repository_uri" {}
variable "subnet_cidr" {
  default = "10.0.0.0/24"
}
variable "buckets_force_destroy" {
  default = false
}
