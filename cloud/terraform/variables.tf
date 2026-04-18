variable "project_id" {
  description = "GCP project ID hosting the Velib data platform."
  type        = string
}

variable "region" {
  description = "Default region for all regional resources (GCS, BigQuery, Dataproc Serverless)."
  type        = string
  default     = "europe-west1"
}

variable "environment" {
  description = "Environment label applied to every resource (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "bronze_bucket_name" {
  description = "Name of the Bronze GCS bucket. Must be globally unique."
  type        = string
}

variable "silver_dataset_id" {
  description = "BigQuery dataset hosting Silver tables."
  type        = string
  default     = "velib_silver"
}

variable "gold_dataset_id" {
  description = "BigQuery dataset hosting Gold (dbt) models."
  type        = string
  default     = "velib_gold"
}

variable "service_account_id" {
  description = "Short name of the pipeline service account (without the @project suffix)."
  type        = string
  default     = "velib-pipeline-sa"
}

variable "labels" {
  description = "Common labels applied to every resource. Keys/values must match GCP label format."
  type        = map(string)
  default = {
    project    = "velib"
    managed_by = "terraform"
  }
}
