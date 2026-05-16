variable "project_id" {}
variable "region" { default = "europe-west3" }
variable "location" { default = "europe-west3" }
variable "gcs_bucket_name" {}
variable "bq_dataset_name" {}
variable "buckets_force_destroy" { default = false }
