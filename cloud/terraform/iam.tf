// Single service account used by Airflow (local orchestrator) and by the
// Dataproc Serverless batches submitted on its behalf. Permissions are
// scoped as tightly as possible: bucket-level on GCS, dataset-level on
// BigQuery, project-level only where Dataproc Serverless requires it.

resource "google_service_account" "pipeline_sa" {
  account_id   = var.service_account_id
  display_name = "Velib pipeline service account"
  description  = "Used by Airflow and Dataproc Serverless batches for the Velib pipeline."

  depends_on = [google_project_service.required]
}

// GCS: objectAdmin on the Bronze bucket only (not project-wide).
resource "google_storage_bucket_iam_member" "pipeline_sa_bronze" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// BigQuery: dataEditor on each dataset only (not project-wide).
resource "google_bigquery_dataset_iam_member" "pipeline_sa_silver" {
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_sa_gold" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// BigQuery jobUser at project level: required to run query/load jobs.
// dataEditor alone does not grant the right to start a job.
resource "google_project_iam_member" "pipeline_sa_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// BigQuery readSessionUser at project level: the Spark BigQuery connector
// uses the BigQuery Storage Read API for direct-mode reads, which requires
// bigquery.readsessions.create on the *project that hosts the API call*
// (not the dataset). dataEditor does not include it.
resource "google_project_iam_member" "pipeline_sa_bq_readsessions" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// Dataproc Serverless needs both editor (submit batches) and worker
// (run the batch on behalf of the SA). Project-scoped by design: the
// Dataproc API does not offer a finer resource level for Serverless.
resource "google_project_iam_member" "pipeline_sa_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_sa_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// Allow the human operator (running Airflow locally) to impersonate
// the SA when submitting batches. The operator principal is passed
// in as a variable so we avoid hard-coding an email.
resource "google_service_account_iam_member" "operator_token_creator" {
  count              = var.operator_principal == "" ? 0 : 1
  service_account_id = google_service_account.pipeline_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = var.operator_principal
}

// Dataproc Serverless batches submitted under the keyfile's identity
// run as GCP_DATAPROC_SERVICE_ACCOUNT (i.e. pipeline_sa itself). The
// submitting principal must hold iam.serviceAccountUser on the target
// SA, even when the two are the same SA — GCP does not grant this
// implicitly. Without this binding, create_batch fails with
// "User not authorized to act as service account ...".
resource "google_service_account_iam_member" "pipeline_sa_self_user" {
  service_account_id = google_service_account.pipeline_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
