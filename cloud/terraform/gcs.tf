// Bronze bucket — raw Velib snapshots landed by the ingestion DAG.
// Lifecycle tiers keep long-tail cost predictable on a <20 EUR/month budget:
// hot 30d on Standard, warm 60d on Nearline, cold 275d on Coldline, deleted at 365d.

resource "google_storage_bucket" "bronze" {
  name                        = var.bronze_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = false

  // Snapshots are append-only and re-ingestible from the Velib API,
  // so versioning would only inflate storage cost.
  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_labels

  depends_on = [google_project_service.required]
}
