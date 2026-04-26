#!/usr/bin/env bash
# Upload the Spark job sources to the Bronze GCS bucket so Dataproc
# Serverless batches can pick them up.
#
# Requires:
#   - gsutil on PATH (Google Cloud SDK)
#   - GCP_BRONZE_BUCKET env var (sourced from .env.cloud by the Makefile)
#
# Usage:
#   ./deploy_spark_job.sh                # uses $GCP_BRONZE_BUCKET
#   ./deploy_spark_job.sh gs://my-bucket  # override target bucket
set -euo pipefail

BUCKET_OVERRIDE="${1:-}"
if [[ -n "${BUCKET_OVERRIDE}" ]]; then
    TARGET="${BUCKET_OVERRIDE%/}/spark_jobs"
else
    : "${GCP_BRONZE_BUCKET:?GCP_BRONZE_BUCKET is required (source .env.cloud or pass a bucket URI)}"
    TARGET="gs://${GCP_BRONZE_BUCKET}/spark_jobs"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SPARK_SRC="${REPO_ROOT}/spark_jobs"

if [[ ! -d "${SPARK_SRC}" ]]; then
    echo "ERROR: Spark source dir not found: ${SPARK_SRC}" >&2
    exit 1
fi

echo "Syncing ${SPARK_SRC} -> ${TARGET}"
# rsync keeps the remote prefix in lockstep with local files; -x excludes
# local bytecode that the Dataproc runtime does not need.
gsutil -m rsync -r -x '(^|/)__pycache__($|/)|\.pyc$' "${SPARK_SRC}" "${TARGET}"
echo "Done."
