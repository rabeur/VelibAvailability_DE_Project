// Dataproc Serverless configuration. Implemented in step 2 of the migration.
// No persistent cluster. Batches are submitted on demand by Airflow via
// DataprocCreateBatchOperator. This file will host reusable batch
// templates / default network config if needed.
//
// Planned shape:
//   - 2 executors, 4 GB memory each (starting point, tune against real volumes)
//   - Runtime version: 2.2 LTS
//   - Service account: google_service_account.pipeline_sa.email
//   - Network: default VPC (keep it simple, no NAT gateway)
