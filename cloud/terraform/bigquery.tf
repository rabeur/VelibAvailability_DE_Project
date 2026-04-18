// BigQuery datasets. Implemented in step 2 of the migration.
// Planned shape:
//   - velib_silver: location europe-west1, default_table_expiration_ms = null,
//     tables partitioned by DAY on ingestion_timestamp, clustered on stationcode.
//   - velib_gold: same location, dbt-managed tables (no Terraform-managed tables here).
//   - labels: local.common_labels
