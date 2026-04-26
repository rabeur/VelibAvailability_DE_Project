# Cloud — GCP migration of the Velib pipeline

Parallel GCP deployment branch (GCS, Dataproc Serverless, BigQuery,
Looker Studio). The local pipeline remains the default target: everything
under this folder is activated via `PIPELINE_TARGET=cloud` and the
`.env.cloud` variables.

## Contents

```
cloud/
├── terraform/          # IaC (gcs, bigquery, dataproc, iam)
├── scripts/            # deployment and manual submission helpers
├── docs/               # spec, setup, cost management
└── README.md           # this file
```

## Prerequisites

- A dedicated GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- `terraform` >= 1.6
- A GCP budget set at 20 EUR with 50 / 80 / 100 % alerts

## Quick start

```bash
cp .env.cloud.example .env.cloud
# fill in GCP_PROJECT_ID, GCP_REGION, etc.

cd cloud/terraform
cp terraform.tfvars.example terraform.tfvars
# fill in project_id, region

terraform init
terraform plan        # review the diff before any apply
```

No command in this repo runs `terraform apply` automatically. The plan
must be reviewed explicitly. See `cloud/docs/setup.md`.

## Guardrails

- Dataproc Serverless only (no permanent cluster)
- Bronze bucket with lifecycle Nearline 30d / Coldline 90d / delete 365d
- BigQuery: partitioning and clustering mandatory, no `SELECT *`
- Run `make cloud-down` at the end of a dev session to leave only storage
- No GCP resource outside Terraform (if it lives in GCP, it lives here)

## Full spec

See `docs/architecture_target.md`.
