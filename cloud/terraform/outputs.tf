// Outputs are populated as resources come online in step 2.
// Keeping them declared (even if commented) documents the contract
// consumed by the Airflow DAGs and the deploy scripts.

// output "bronze_bucket_name" {
//   description = "Name of the Bronze GCS bucket."
//   value       = google_storage_bucket.bronze.name
// }

// output "silver_dataset_id" {
//   description = "BigQuery Silver dataset ID."
//   value       = google_bigquery_dataset.silver.dataset_id
// }

// output "gold_dataset_id" {
//   description = "BigQuery Gold dataset ID."
//   value       = google_bigquery_dataset.gold.dataset_id
// }

// output "pipeline_service_account_email" {
//   description = "Email of the pipeline service account used by Airflow and Dataproc."
//   value       = google_service_account.pipeline_sa.email
// }
