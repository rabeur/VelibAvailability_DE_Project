#!/usr/bin/env bash
# Submit (or inspect) a Dataproc Serverless batch running the Bronze-to-
# Silver Spark job. Kept outside Airflow so the job can be rerun manually
# during migration debugging without waking up the scheduler.
#
# Usage:
#   ./run_dataproc_batch.sh                 # submit a new batch
#   ./run_dataproc_batch.sh --logs-only     # describe the most recent batch
#
# Required env (from .env.cloud):
#   GCP_PROJECT_ID, GCP_REGION, GCP_BRONZE_BUCKET
set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required (source .env.cloud)}"
: "${GCP_REGION:?GCP_REGION is required (source .env.cloud)}"

MODE="${1:-submit}"

if [[ "${MODE}" == "--logs-only" ]]; then
    # The list is already sorted most-recent-first by the API.
    latest="$(gcloud dataproc batches list \
        --region="${GCP_REGION}" \
        --project="${GCP_PROJECT_ID}" \
        --format='value(name)' \
        --limit=1)"
    if [[ -z "${latest}" ]]; then
        echo "No Dataproc Serverless batch found in ${GCP_REGION}." >&2
        exit 1
    fi
    # name is the fully-qualified resource path; the CLI needs the id.
    batch_id="${latest##*/}"
    gcloud dataproc batches describe "${batch_id}" \
        --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
    exit 0
fi

: "${GCP_BRONZE_BUCKET:?GCP_BRONZE_BUCKET is required to submit a batch}"

BATCH_ID="velib-bronze-to-silver-$(date -u +%Y%m%d-%H%M%S)"
SA="velib-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
MAIN_PY="gs://${GCP_BRONZE_BUCKET}/spark_jobs/bronze_to_silver.py"
PY_FILES="gs://${GCP_BRONZE_BUCKET}/spark_jobs/silver_writers.py,gs://${GCP_BRONZE_BUCKET}/spark_jobs/paris_arrondissement_utils.py"

echo "Submitting Dataproc Serverless batch: ${BATCH_ID}"
# 2 executors / 4 GB each matches the initial sizing spec in
# cloud/docs/architecture_target.md. Runtime 2.2 pins the Spark/Python
# versions so the local job binary stays reproducible.
gcloud dataproc batches submit pyspark \
    "${MAIN_PY}" \
    --batch="${BATCH_ID}" \
    --region="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --service-account="${SA}" \
    --version=2.2 \
    --py-files="${PY_FILES}" \
    --properties="spark.executor.instances=2,spark.executor.memory=4g" \
    -- \
    --target=bigquery \
    --bronze-bucket="${GCP_BRONZE_BUCKET}"
