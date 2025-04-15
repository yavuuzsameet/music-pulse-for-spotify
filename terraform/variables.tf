variable "project_id" {
  description = "The project ID"
  type        = string
}

variable "region" {
  description = "The primary Google Cloud region for resources"
  type        = string
  default     = "europe-west3"
}

variable "location" {
  description = "The primary Google Cloud location for resources"
  type        = string
  default     = "EU"
}

variable "gcs_bucket_name" {
  description = "The globally unique name for the GCS data lake bucket"
  type        = string
}

variable "bq_dataset_id" {
  description = "The ID for the BigQuery Data Warehouse dataset"
  type        = string
  default     = "music_pulse_warehouse"
}
