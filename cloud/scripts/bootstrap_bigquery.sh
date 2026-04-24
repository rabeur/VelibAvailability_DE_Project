#!/usr/bin/env bash
# Sanity-check the BigQuery datasets that Terraform created. Silver tables
# themselves are materialised on first write by the Spark job (BigQuery
# auto-creates tables when writing a partitioned/clustered DataFrame), so
# there is no DDL to run here. Gold tables are owned by dbt.
#
# The script stays idempotent: running it twice produces identical output.
#
# Required env: GCP_PROJECT_ID, GCP_BIGQUERY_SILVER_DATASET, GCP_BIGQUERY_GOLD_DATASET.
set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required (source .env.cloud)}"
: "${GCP_BIGQUERY_SILVER_DATASET:=velib_silver}"
: "${GCP_BIGQUERY_GOLD_DATASET:=velib_gold}"

check_dataset() {
    local dataset="$1"
    if bq --project_id="${GCP_PROJECT_ID}" ls "${dataset}" >/dev/null 2>&1; then
        echo "OK: ${GCP_PROJECT_ID}:${dataset} exists"
    else
        echo "MISSING: ${GCP_PROJECT_ID}:${dataset} — run 'make cloud-up' first" >&2
        return 1
    fi
}

check_dataset "${GCP_BIGQUERY_SILVER_DATASET}"
check_dataset "${GCP_BIGQUERY_GOLD_DATASET}"
echo "BigQuery bootstrap check complete."
