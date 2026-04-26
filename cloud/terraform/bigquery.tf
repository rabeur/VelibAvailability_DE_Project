// Silver and Gold datasets. Tables themselves are created by Spark
// (Silver) and dbt (Gold) — Terraform only owns the dataset shells,
// their location, labels, and default expirations.

resource "google_bigquery_dataset" "silver" {
  dataset_id    = var.silver_dataset_id
  friendly_name = "Velib Silver"
  description   = "Cleaned and conformed Velib snapshots. Populated by the Dataproc Serverless Bronze-to-Silver job."
  location      = var.region

  // No default table expiration: Silver is the source of truth for Gold.
  // Retention is governed at table level (partition expiration) when needed.
  default_table_expiration_ms = null

  labels = local.common_labels

  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset" "gold" {
  dataset_id    = var.gold_dataset_id
  friendly_name = "Velib Gold"
  description   = "Analytics-ready marts produced by dbt (bigquery_cloud profile)."
  location      = var.region

  default_table_expiration_ms = null

  labels = local.common_labels

  depends_on = [google_project_service.required]
}
