variable "project_id" {}
variable "region" { default = "europe-west3" }
variable "location" { default = "EU" }
variable "vpc_name" {}
variable "subnet_name" {}
variable "compute_router_name" {}
variable "router_nat_name" {}
variable "bq_dataset_name" {}
variable "gcs_bucket_name" {}
variable "dataproc_cluster_name" {}
variable "dataproc_image_version" {
  default = "2.3-debian12"
}
variable "dataproc_master_node_machine_type" {
  default = "e2-standard-2"
}
variable "dataproc_worker_node_machine_type" {
  default = "e2-standard-2"
}
variable "dataproc_master_node_number_of_instances" {
  default = 1
}
variable "dataproc_worker_node_number_of_instances" {
  default = 2
}
variable "dataproc_master_node_disk_size_gb" {
    default = 32
}
variable "dataproc_worker_node_disk_size_gb" {
    default = 32
}
variable "subnet_cidr" {
  default = "10.0.0.0/24"
}
variable "staging_bucket_retention_days" {
  default = 7
}
variable "buckets_force_destroy" {
  default = false
}
variable "init_script_timeout_sec" {
  default = 600
}
