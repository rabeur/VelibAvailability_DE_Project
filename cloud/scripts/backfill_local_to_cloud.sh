#!/usr/bin/env bash
# Backfill the local Bronze data lake to GCS, then run Bronze->Silver
# (Dataproc Serverless) and Silver->Gold (dbt) only on the ingestion days
# that are not yet materialised in BigQuery.
#
# Default mode is dry-run: nothing is created, transferred or billed.
# Use --apply to actually run the pipeline. This script never destroys
# data: rsync is one-way (local -> GCS) and Spark/dbt are append-only.
#
# Usage:
#   ./backfill_local_to_cloud.sh                 # dry-run (default)
#   ./backfill_local_to_cloud.sh --apply         # execute end to end
#   ./backfill_local_to_cloud.sh --apply \
#       --skip-sync --skip-gold                  # partial run
#   ./backfill_local_to_cloud.sh --from=2026-04-01 --to=2026-04-29 --apply
#
# Required env (sourced from .env.cloud at the repo root):
#   GCP_PROJECT_ID, GCP_REGION, GCP_BRONZE_BUCKET,
#   GCP_BIGQUERY_SILVER_DATASET, GCP_BIGQUERY_GOLD_DATASET,
#   GCP_DATAPROC_SERVICE_ACCOUNT, GCP_SERVICE_ACCOUNT_KEY
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_CLOUD="${REPO_ROOT}/.env.cloud"

# ---- args ------------------------------------------------------------------

APPLY=0
SKIP_SYNC=0
SKIP_SILVER=0
SKIP_GOLD=0
DATE_FROM=""
DATE_TO=""

for arg in "$@"; do
    case "${arg}" in
        --apply)        APPLY=1 ;;
        --dry-run)      APPLY=0 ;;
        --skip-sync)    SKIP_SYNC=1 ;;
        --skip-silver)  SKIP_SILVER=1 ;;
        --skip-gold)    SKIP_GOLD=1 ;;
        --from=*)       DATE_FROM="${arg#*=}" ;;
        --to=*)         DATE_TO="${arg#*=}" ;;
        -h|--help)
            sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *)
            echo "Unknown argument: ${arg}" >&2
            exit 2 ;;
    esac
done

# ---- env -------------------------------------------------------------------

if [[ ! -f "${ENV_CLOUD}" ]]; then
    echo "ERROR: ${ENV_CLOUD} is missing. Copy .env.cloud.example and fill it." >&2
    exit 1
fi
# Source .env.cloud safely: variables defined there land in the current shell.
set -a
# shellcheck disable=SC1090
. "${ENV_CLOUD}"
set +a

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required}"
: "${GCP_REGION:?GCP_REGION is required}"
: "${GCP_BRONZE_BUCKET:?GCP_BRONZE_BUCKET is required}"
: "${GCP_BIGQUERY_SILVER_DATASET:=velib_silver}"
: "${GCP_BIGQUERY_GOLD_DATASET:=velib_gold}"

LOCAL_BRONZE="${REPO_ROOT}/data_lake/bronze/velib"
GCS_BRONZE="gs://${GCP_BRONZE_BUCKET}/bronze/velib"
SILVER_TABLE="${GCP_PROJECT_ID}.${GCP_BIGQUERY_SILVER_DATASET}.station_availability"

mode_label="DRY-RUN"; [[ ${APPLY} -eq 1 ]] && mode_label="APPLY"

cat <<EOF
============================================================
 Velib backfill: local data lake -> GCS -> BigQuery
 Mode             : ${mode_label}
 Local bronze     : ${LOCAL_BRONZE}
 GCS bronze       : ${GCS_BRONZE}
 BigQuery silver  : ${SILVER_TABLE}
 Date window      : ${DATE_FROM:-<all>} .. ${DATE_TO:-<all>}
============================================================
EOF

run() {
    # Echo the command, run it only on --apply. The if/fi form (instead of
    # `[[ ... ]] && cmd`) matters under `set -e`: a failing test would
    # otherwise return 1 and abort the whole script in dry-run mode.
    echo "+ $*"
    if [[ ${APPLY} -eq 1 ]]; then
        eval "$@"
    fi
}

confirm() {
    # Cheap typed confirmation for cost-bearing operations.
    [[ ${APPLY} -eq 0 ]] && return 0
    local prompt="$1" expected="$2"
    read -r -p "${prompt} (type '${expected}' to continue): " ans
    [[ "${ans}" == "${expected}" ]] || { echo "Aborted."; exit 1; }
}

# ---- 1. sync local bronze -> GCS -------------------------------------------

if [[ ${SKIP_SYNC} -eq 0 ]]; then
    echo
    echo "[1/4] Syncing local bronze to GCS"
    if [[ ! -d "${LOCAL_BRONZE}" ]]; then
        echo "ERROR: local bronze dir not found: ${LOCAL_BRONZE}" >&2
        exit 1
    fi

    # Volume preview so the user sees the order of magnitude before confirming.
    bytes="$(du -sb "${LOCAL_BRONZE}" 2>/dev/null | awk '{print $1}')"
    human="$(du -sh "${LOCAL_BRONZE}" 2>/dev/null | awk '{print $1}')"
    file_count="$(find "${LOCAL_BRONZE}" -type f -name '*.parquet' | wc -l)"
    echo "  Local size : ${human} (${bytes} bytes)"
    echo "  Parquet    : ${file_count} files"

    confirm "About to upload ${human} to ${GCS_BRONZE}" "sync"

    # rsync is incremental: existing identical objects are skipped, so re-runs
    # only push diffs. -x excludes pycache and the local 'test/' fixtures dir
    # which has no business in the cloud bucket.
    run gsutil -m rsync -r \
        -x "'(^|/)__pycache__($|/)|\.pyc$|(^|/)test(/|$)'" \
        "${LOCAL_BRONZE}" "${GCS_BRONZE}"
else
    echo
    echo "[1/4] Skipping sync (--skip-sync)"
fi

# ---- 2. compute the set of ingestion_date to process -----------------------

echo
echo "[2/4] Computing ingestion_date diff (GCS vs BigQuery)"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

gcs_dates="${tmp_dir}/gcs_dates.txt"
bq_dates="${tmp_dir}/bq_dates.txt"
todo_dates="${tmp_dir}/todo_dates.txt"

# Source of truth for "what data is on GCS": the partition prefixes. We list
# directly on GCS (not on local) so re-runs after a partial sync still see
# the real cloud state.
gsutil ls -d "${GCS_BRONZE}/ingestion_date=*" 2>/dev/null \
    | sed -n 's|.*/ingestion_date=\([0-9-]\{10\}\)/\?$|\1|p' \
    | sort -u > "${gcs_dates}"

gcs_count=$(wc -l < "${gcs_dates}" | tr -d ' ')
echo "  GCS ingestion days : ${gcs_count}"

# Heads-up: the diff above reflects the *current* GCS state. Under --apply
# the sync runs first, so the real diff will include freshly-uploaded days.
if [[ ${APPLY} -eq 0 && ${SKIP_SYNC} -eq 0 ]]; then
    local_days=$(find "${LOCAL_BRONZE}" -mindepth 1 -maxdepth 1 \
        -type d -name 'ingestion_date=*' 2>/dev/null | wc -l)
    echo "  Local ingestion days: ${local_days} (some may not be on GCS yet -- sync runs in --apply)"
fi

# BigQuery: list days already materialised. INFORMATION_SCHEMA.PARTITIONS
# is metadata-only (free and fast) but lags behind by minutes on freshly
# written partitions, which makes the diff loop re-submit batches that
# just succeeded. Querying the data is authoritative and cheap: BigQuery
# only reads the partition-key column (~8 bytes/row), well below the
# 1 TiB/month free tier. The table may not exist on the very first run.
bq_query="SELECT FORMAT_DATE('%Y-%m-%d', DATE(snapshot_timestamp, 'Europe/Paris')) AS d \
FROM \`${SILVER_TABLE}\` \
GROUP BY d \
ORDER BY d"

# bq show parses fully-qualified refs as project:dataset.table (colon, not
# dot) even with --project_id; passing "project.dataset.table" misroutes
# the lookup and returns "not found" silently, which would re-submit every
# batch. Pass dataset.table only.
if bq --project_id="${GCP_PROJECT_ID}" show --format=prettyjson \
       "${GCP_BIGQUERY_SILVER_DATASET}.station_availability" >/dev/null 2>&1; then
    bq --project_id="${GCP_PROJECT_ID}" query \
        --use_legacy_sql=false --format=csv --quiet "${bq_query}" \
        | tail -n +2 > "${bq_dates}"
else
    echo "  (Silver table not found yet, treating as empty)"
    : > "${bq_dates}"
fi

bq_count=$(wc -l < "${bq_dates}" | tr -d ' ')
echo "  BQ days already in : ${bq_count}"

# Days present on GCS but missing from BigQuery.
comm -23 "${gcs_dates}" <(sort -u "${bq_dates}") > "${todo_dates}"

# Optional date-window filter.
if [[ -n "${DATE_FROM}" || -n "${DATE_TO}" ]]; then
    awk -v f="${DATE_FROM}" -v t="${DATE_TO}" \
        '(!f || $0 >= f) && (!t || $0 <= t)' \
        "${todo_dates}" > "${todo_dates}.win" && mv "${todo_dates}.win" "${todo_dates}"
fi

todo_count=$(wc -l < "${todo_dates}" | tr -d ' ')
echo "  Days to process    : ${todo_count}"
if [[ ${todo_count} -gt 0 ]]; then
    echo "  Pending list:"
    sed 's/^/    - /' "${todo_dates}"
fi

# ---- 3. submit one Dataproc Serverless batch per missing day ---------------

if [[ ${SKIP_SILVER} -eq 0 && ${todo_count} -gt 0 ]]; then
    echo
    echo "[3/4] Submitting Dataproc Serverless batches (one per day)"

    SA="${GCP_DATAPROC_SERVICE_ACCOUNT:-velib-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com}"
    MAIN_PY="gs://${GCP_BRONZE_BUCKET}/spark_jobs/bronze_to_silver.py"
    PY_FILES="gs://${GCP_BRONZE_BUCKET}/spark_jobs/silver_writers.py,gs://${GCP_BRONZE_BUCKET}/spark_jobs/paris_arrondissement_utils.py"
    # paris_arrondissement_utils.load_arrondissement_boundaries() reads a
    # geojson sibling. --py-files only distributes Python modules, so the
    # data file must be shipped via --files; it lands in the worker cwd
    # (next to the .py modules) which matches the module's fallback path.
    EXTRA_FILES="gs://${GCP_BRONZE_BUCKET}/spark_jobs/data/paris_arrondissements.geojson"

    # Guard against forgetting to upload the Spark job. Free, no-cost check.
    if [[ ${APPLY} -eq 1 ]] && ! gsutil -q stat "${MAIN_PY}"; then
        echo "ERROR: ${MAIN_PY} is missing. Run 'make cloud-deploy-spark' first." >&2
        exit 1
    fi

    confirm "About to submit ${todo_count} Dataproc Serverless batches" "submit"

    # Sequential submission: simpler ordering, easier failure recovery, and
    # well within the 60 batches/day Dataproc Serverless quota. Each batch
    # processes a single ingestion_date so failures are isolated.
    failed=0
    while IFS= read -r d; do
        [[ -z "${d}" ]] && continue
        batch_id="velib-b2s-${d//-/}-$(date -u +%H%M%S)"
        echo
        echo "  -> ${d}  (batch ${batch_id})"
        if ! run gcloud dataproc batches submit pyspark \
                "${MAIN_PY}" \
                --batch="${batch_id}" \
                --region="${GCP_REGION}" \
                --project="${GCP_PROJECT_ID}" \
                --service-account="${SA}" \
                --version=2.2 \
                --py-files="${PY_FILES}" \
                --files="${EXTRA_FILES}" \
                --properties="spark.executor.instances=2,spark.executor.memory=4g" \
                -- \
                --target=cloud \
                --bronze-bucket="${GCP_BRONZE_BUCKET}" \
                --gcp-project="${GCP_PROJECT_ID}" \
                --silver-dataset="${GCP_BIGQUERY_SILVER_DATASET}" \
                "${d}" ; then
            echo "  !! batch failed for ${d}"
            failed=$((failed + 1))
        fi
    done < "${todo_dates}"

    if [[ ${failed} -gt 0 ]]; then
        echo
        echo "ERROR: ${failed} batch(es) failed. Skipping Gold step." >&2
        exit 1
    fi
else
    echo
    if [[ ${SKIP_SILVER} -eq 1 ]]; then
        echo "[3/4] Skipping Silver step (--skip-silver)"
    else
        echo "[3/4] Nothing to process for Silver"
    fi
fi

# ---- 4. dbt Gold against BigQuery ------------------------------------------

if [[ ${SKIP_GOLD} -eq 0 ]]; then
    echo
    echo "[4/4] Running dbt Gold against BigQuery"

    # The dbt container ('velib_dbt') must be running with the cloud env
    # mounted (keyfile + GCP_* vars). 'make cloud-stack-up' takes care of it.
    if ! docker ps --format '{{.Names}}' | grep -qx velib_dbt; then
        echo "WARNING: container 'velib_dbt' is not running."
        echo "         Run 'make cloud-stack-up' before --apply, or use --skip-gold."
        [[ ${APPLY} -eq 1 ]] && exit 1
    fi

    confirm "About to run dbt run + dbt test against ${GCP_BIGQUERY_GOLD_DATASET}" "dbt"

    run docker exec -e PIPELINE_TARGET=cloud velib_dbt \
        dbt run --profiles-dir /usr/app/dbt --target bigquery_cloud
    run docker exec -e PIPELINE_TARGET=cloud velib_dbt \
        dbt test --profiles-dir /usr/app/dbt --target bigquery_cloud
else
    echo
    echo "[4/4] Skipping Gold step (--skip-gold)"
fi

echo
if [[ ${APPLY} -eq 1 ]]; then
    echo "Backfill completed."
else
    echo "Dry-run finished. Re-run with --apply to execute."
fi
