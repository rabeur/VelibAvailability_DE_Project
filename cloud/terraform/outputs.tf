output "bronze_bucket_name" {
  description = "Name of the Bronze GCS bucket."
  value       = google_storage_bucket.bronze.name
}

output "bronze_bucket_url" {
  description = "gs:// URL of the Bronze bucket, consumed by Spark jobs and Airflow."
  value       = "gs://${google_storage_bucket.bronze.name}"
}

output "silver_dataset_id" {
  description = "BigQuery Silver dataset ID."
  value       = google_bigquery_dataset.silver.dataset_id
}

output "gold_dataset_id" {
  description = "BigQuery Gold dataset ID."
  value       = google_bigquery_dataset.gold.dataset_id
}

output "pipeline_service_account_email" {
  description = "Email of the pipeline service account used by Airflow and Dataproc."
  value       = google_service_account.pipeline_sa.email
}
