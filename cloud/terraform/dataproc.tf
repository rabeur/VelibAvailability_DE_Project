// Dataproc Serverless: no permanent cluster by design. Batches are
// submitted on demand by Airflow (DataprocCreateBatchOperator). This
// file intentionally declares no resource — defaults live with the
// caller (DAG / run_dataproc_batch.sh) so that tuning executors or
// runtime version does not require a `terraform apply`.
//
// Kept as a documented placeholder: if we later introduce a reusable
// batch template (google_dataproc_workflow_template) or a dedicated
// VPC subnet for Serverless egress, they belong here.
//
// Default batch shape (applied by the caller):
//   - runtime version: 2.2 LTS
//   - executors: 2, 4 GB memory each
//   - service account: google_service_account.pipeline_sa.email
//   - network: default VPC (no NAT, no custom subnet)
