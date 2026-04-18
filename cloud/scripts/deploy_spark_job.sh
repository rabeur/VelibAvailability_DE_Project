#!/usr/bin/env bash
# Upload the Bronze-to-Silver Spark job to the configured GCS bucket so
# Dataproc Serverless can pick it up. Implemented in step 2.
#
# Usage (planned):
#   ./deploy_spark_job.sh            # uses $GCP_BRONZE_BUCKET from .env.cloud
#   ./deploy_spark_job.sh gs://bkt   # override target bucket
set -euo pipefail
echo "deploy_spark_job.sh: not implemented yet (step 2)"
exit 1
