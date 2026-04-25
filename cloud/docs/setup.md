# GCP setup

Onboarding guide to provision the cloud branch of the Velib pipeline.
Goal: get from an empty GCP project to a valid `terraform plan` in
under 15 minutes.

## Prerequisites

On your machine:

- Recent `gcloud` CLI (>= 470), authenticated
- `terraform` >= 1.6
- A Google account with permission to create a GCP project and enable billing

On the GCP side: a payment method attached to a billing account. The free tier covers most resources for testing, but billing must be active for the Dataproc API to respond.

## 1. Create the GCP project

```bash
gcloud projects create velib-analytics-<suffix> --name="Velib Analytics"
gcloud config set project velib-analytics-<suffix>
```

The suffix must be globally unique (three or four letters is enough). This `project_id` is reused as-is in `terraform.tfvars`.

## 2. Link billing

```bash
gcloud billing accounts list
gcloud billing projects link velib-analytics-<suffix> \
  --billing-account=<BILLING_ACCOUNT_ID>
```

Without this step, Terraform will fail as soon as it tries to enable the Dataproc API.

## 3. Configure a budget and alerts

Recommended before any `apply`. See `cost_management.md` for details. Minimal version:

```bash
gcloud billing budgets create \
  --billing-account=<BILLING_ACCOUNT_ID> \
  --display-name="velib-budget" \
  --budget-amount=20EUR \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.8 \
  --threshold-rule=percent=1.0
```

## 4. Authenticate gcloud for Terraform

```bash
gcloud auth application-default login
```

Terraform uses the "application default credentials": no service account key file is needed for the initial `apply`. Keys are only useful for Airflow (local execution driving the cloud).

## 5. Configure Terraform variables

```bash
cd cloud/terraform
cp terraform.tfvars.example terraform.tfvars
```

At minimum, fill in `project_id` and `bronze_bucket_name` (must be globally unique, typically `velib-bronze-<project_id>`). Other variables have sensible defaults.

If you want to impersonate the service account from your machine, set `operator_principal = "user:your.email@gmail.com"`.

## 6. Init and plan

```bash
terraform init
terraform validate
terraform plan -out=tfplan
```

Review the plan. You should see:

- 5 API enablements (`google_project_service.required`)
- 1 GCS bucket with 3 lifecycle rules
- 2 BigQuery datasets
- 1 service account + 8 IAM bindings (9 if `operator_principal` is set)
- No costly permanent resource such as cluster, VM, Cloud SQL, or NAT

The 8 bindings cover: `storage.objectAdmin` on the Bronze bucket,
`bigquery.dataEditor` on each dataset (×2), project-level `bigquery.jobUser`
and `bigquery.readSessionUser` (the latter needed by the Spark BigQuery
connector's Storage Read API), `dataproc.editor` and `dataproc.worker`
project-level, and `iam.serviceAccountUser` self-binding so the SA can
submit Dataproc batches that run as itself. The optional 9th is
`iam.serviceAccountTokenCreator` for the human operator.

## 7. Apply

```bash
terraform apply tfplan
```

Duration: roughly 2 to 3 minutes, mostly API enablement. The bucket and datasets are created in seconds.

## 8. Post-apply checks

```bash
terraform output

gsutil ls -L -b gs://$(terraform output -raw bronze_bucket_name) | head -20
bq ls --location=europe-west1

gcloud iam service-accounts list --filter="email:velib-pipeline-sa@*"
```

All three commands must return the created resources without any permission error.

## 9. Generate a service account key for Airflow

Only if local Airflow must submit Dataproc jobs and write to BigQuery without impersonation:

```bash
gcloud iam service-accounts keys create ~/.config/gcloud/velib-pipeline-sa.json \
  --iam-account=velib-pipeline-sa@<project_id>.iam.gserviceaccount.com
```

Store this file outside the repo. It is referenced by `GCP_SERVICE_ACCOUNT_KEY` in `.env.cloud`.

Alternative without a key: configure impersonation via `operator_principal` and use the SA with `GOOGLE_APPLICATION_CREDENTIALS` unset. Prefer impersonation whenever possible (no long-lived secret).

## 10. Run the pipeline in cloud mode

Once Terraform has applied, the local Docker stack can flip to cloud
mode. The same Airflow scheduler and dbt container drive both targets;
the switch is `PIPELINE_TARGET=cloud` plus the GCS / BigQuery env vars,
all sourced exclusively by `cloud-*` Make targets.

```bash
cp .env.cloud.example .env.cloud
# fill in GCP_PROJECT_ID, GCP_REGION, GCP_BRONZE_BUCKET,
# GCP_BIGQUERY_SILVER_DATASET, GCP_BIGQUERY_GOLD_DATASET,
# GCP_SERVICE_ACCOUNT_KEY (absolute path to the JSON keyfile),
# GCP_DATAPROC_SERVICE_ACCOUNT (the velib-pipeline-sa email),
# and PIPELINE_TARGET=cloud.
chmod 644 "$GCP_SERVICE_ACCOUNT_KEY"
```

The keyfile must be world-readable: containers run as UID 50000
(Airflow) and a non-root UID for `velib_dbt`. A 0600 keyfile will fail
to open inside the container with `PermissionError`.

```bash
make build              # rebuild the dbt image so it ships dbt-bigquery
make cloud-deploy-spark # rsync spark_jobs/ to gs://<bronze>/spark_jobs/
make cloud-stack-up     # docker compose up with .env.cloud sourced
```

`make cloud-stack-up` is the cloud equivalent of `make up`: it starts
the same containers but injects `PIPELINE_TARGET=cloud` and the GCP
variables so DAGs branch to GCS / Dataproc / BigQuery. Conversely,
plain `make up` always runs local mode regardless of what sits in
`.env.cloud`.

In the Airflow UI (http://localhost:8081), unpause and trigger the
DAGs in this order:

1. `velib_ingestion_pipeline` — writes minute-level snapshots to
   `gs://<bronze>/bronze/velib/...`
2. `velib_data_quality` — produces a CSV report in
   `gs://<bronze>/reports/data_quality/...`
3. `velib_bronze_cleanup_hourly` — auto-triggers the Silver DAG once
   the previous hour partition is clean
4. `velib_silver_transformation_hourly` — submits a Dataproc Serverless
   batch named `velib-silver-YYYYMMDD-HH-XXXXXX` and validates the
   resulting BigQuery rows
5. `velib_dbt_gold_transformation` — runs dbt staging + gold + tests
   on BigQuery via the `bigquery_cloud` profile

Watch the Dataproc batch in the GCP console (or via
`gcloud dataproc batches list --region=europe-west1`); a typical run
processes ~75 k bronze rows in 60 to 90 seconds.

Confirm everything landed:

```bash
bq query --use_legacy_sql=false \
  "SELECT table_name, row_count
   FROM \`<project_id>.velib_silver.__TABLES__\`
   UNION ALL
   SELECT table_name, row_count
   FROM \`<project_id>.velib_gold.__TABLES__\`
   ORDER BY table_name"
```

Both `silver.*` tables and the five `gold.*` tables (plus the two
`stg_*` views materialised in the Gold dataset) should have non-zero
counts.

## 11. Teardown

At the end of a dev session:

```bash
terraform destroy
```

The Bronze bucket is intentionally not `force_destroy = true`. To fully tear down, empty the bucket first:

```bash
gsutil -m rm -r gs://$(terraform output -raw bronze_bucket_name)/**
terraform destroy
```

## Troubleshooting

**"API not enabled" even after a successful `terraform apply`.** Enablement sometimes takes 30 to 60 seconds to propagate. Re-running the command resolves it in 90% of cases.

**IAM "permission denied" when launching a job.** Check that `operator_principal` is set and matches the current user. IAM propagation can take 1 to 2 minutes.

**`terraform destroy` hangs on the bucket.** Empty the objects before destroying (command above). The bucket is protected against accidental deletion by default.

**Dataproc batch fails with `User not authorized to act as service account ...`.** The submitter (the keyfile's identity) lacks `iam.serviceAccountUser` on the Dataproc batch SA. Terraform creates a self-binding so the pipeline SA can act as itself; check it is present (`pipeline_sa_self_user` in `iam.tf`) and that `GCP_DATAPROC_SERVICE_ACCOUNT` in `.env.cloud` is set to the pipeline SA email.

**Dataproc batch driver crashes with `PERMISSION_DENIED: bigquery.readsessions.create`.** The Spark BigQuery connector uses the BigQuery Storage Read API, which needs `roles/bigquery.readSessionUser` at the project level. Terraform owns this binding (`pipeline_sa_bq_readsessions` in `iam.tf`); a missing one usually means the apply did not include it.

**dbt fails with `Access Denied: bigquery.datasets.create`.** dbt's default `generate_schema_name` macro suffixes custom schemas (`+schema: staging` becomes `<gold>_staging`), which would require dataset creation. The repo overrides the macro for BigQuery in `dbt/velib_dbt/macros/generate_schema_name.sql` so staging materialises inside the Gold dataset; if that file is missing or removed, dbt regresses to the default behaviour and trips this permission.

**`PermissionError: /etc/gcp/service_account.json` inside a container.** The keyfile on the host is `0600`. Containers run as UID 50000 (Airflow) or a service-specific UID (dbt) and cannot open it. Run `chmod 644 <keyfile>` on the host; no container restart needed.
