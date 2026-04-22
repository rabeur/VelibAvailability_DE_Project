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
- 1 service account + 6 IAM bindings (7 if `operator_principal` is set)
- No costly permanent resource such as cluster, VM, Cloud SQL, or NAT

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

## 10. Teardown

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
