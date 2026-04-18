// Pipeline service account and least-privilege bindings.
// Implemented in step 2 of the migration.
//
// Planned shape:
//   - google_service_account.pipeline_sa (account_id = var.service_account_id)
//   - roles/storage.objectAdmin scoped to the Bronze bucket only
//     (via google_storage_bucket_iam_member, NOT project-wide)
//   - roles/bigquery.dataEditor on velib_silver and velib_gold datasets
//     (via google_bigquery_dataset_iam_member)
//   - roles/dataproc.editor + roles/dataproc.worker at project level
//     (Dataproc Serverless requires them for batch submission/execution)
