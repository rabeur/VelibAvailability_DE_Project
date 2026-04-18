// Bronze bucket. Implemented in step 2 of the migration.
// Planned shape:
//   - name: var.bronze_bucket_name
//   - location: var.region
//   - uniform_bucket_level_access: true
//   - versioning: disabled (snapshots are append-only, saves cost)
//   - lifecycle: Nearline 30d, Coldline 90d, delete 365d
//   - labels: local.common_labels
//
// Keep this file empty until step 2 to avoid accidental `terraform apply`
// creating storage before the plan has been reviewed.
