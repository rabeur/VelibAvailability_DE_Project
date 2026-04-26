# Target architecture — GCP migration of the Velib pipeline

## Guiding principle

Hybrid strategy: **keep the existing local pipeline strictly intact** and add a parallel cloud deployment branch that can be activated on demand. Both environments (local and GCP) must run independently on the same codebase.

## Target repo layout

```
VelibAvailability_DE_Project/
├── airflow/                      # existing, extended to support both modes
│   └── dags/
│       ├── velib_ingestion_pipeline.py         # existing, extended with optional GCS sink
│       ├── velib_bronze_cleanup_hourly.py      # existing, dual-mode
│       ├── velib_silver_transformation_hourly.py # existing, branches to Dataproc when cloud
│       └── velib_dbt_gold_transformation.py    # existing, parameterized dbt profile
├── spark_jobs/
│   └── bronze_to_silver.py       # existing, abstract I/O (local fs or gs://)
├── dbt/
│   ├── profiles.yml              # two profiles: postgres_local and bigquery_cloud
│   └── models/                   # models compatible with both engines
├── cloud/                        # new folder, all cloud code lives here
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── gcs.tf                # Bronze bucket + lifecycle rules
│   │   ├── bigquery.tf           # silver and gold datasets, partitioning, clustering
│   │   ├── dataproc.tf           # Dataproc Serverless batch configuration
│   │   ├── iam.tf                # service account + least-privilege roles
│   │   └── terraform.tfvars.example
│   ├── scripts/
│   │   ├── deploy_spark_job.sh   # upload the Spark job to GCS
│   │   ├── run_dataproc_batch.sh # manual submission for testing
│   │   └── bootstrap_bigquery.sh # initial schema creation
│   ├── docs/
│   │   ├── setup.md              # GCP setup guide
│   │   ├── cost_management.md    # budgets, alerts, free tier
│   │   └── architecture.md       # target cloud diagram
│   └── README.md                 # cloud-specific README
├── Makefile                      # existing, extended with cloud-* targets
└── .env.cloud.example            # GCP environment variables
```

## Target components and roles

### Google Cloud Storage

Single bucket `velib-bronze-<project_id>` organized with the same partitioning scheme as local:

```
gs://velib-bronze-<project_id>/
├── bronze/velib/
│   ├── ingestion_date=2026-04-16/
│   │   ├── hour=00/
│   │   └── hour=01/
│   └── ...
├── reports/data_quality/
│   └── report_date=2026-04-16/
└── spark_jobs/                  # .py files of the Spark jobs to be executed
```

Lifecycle rules: transition to Nearline after 30 days, Coldline after 90 days, delete after 365 days, to keep costs under control.

### BigQuery

Two datasets in region `europe-west1` (Belgium, close to Paris, moderate cost):

- `velib_silver` — Silver tables partitioned by day on `ingestion_timestamp`, clustered on `stationcode`
- `velib_gold` — dbt Gold models (dimensions, facts, marts)

### Dataproc Serverless

No permanent cluster. Each run of the Bronze-to-Silver job is an on-demand Serverless batch: 60 to 90 seconds startup, per-second billing, autoscaling handled by GCP. Initial configuration: 2 executors, 4 GB memory each, to be tuned against real volumes.

### Service account

A single `velib-pipeline-sa` service account with strictly necessary roles:

- `roles/storage.objectAdmin` scoped to the Bronze bucket
- `roles/bigquery.dataEditor` on both datasets
- `roles/dataproc.editor` to submit batches
- `roles/dataproc.worker` for batch execution

### Local Airflow as orchestrator

Existing DAGs stay in place. Two structural changes:

1. A `PIPELINE_TARGET` environment variable (`local` or `cloud`) switches paths and connectors
2. Adding GCP tasks via native operators: `GCSHook`, `DataprocCreateBatchOperator`, `BigQueryInsertJobOperator`

### dbt

The `profiles.yml` file exposes two targets. SQL models are kept compatible with both engines via Jinja macros where needed (types, date functions).

### Looker Studio

Direct connection to the `velib_gold.*` tables. Public read-only dashboards for demo purposes, mirroring the local Superset views.

## Environment variables

Non-committed `.env.cloud` file:

```
GCP_PROJECT_ID=velib-analytics-xxxxx
GCP_REGION=europe-west1
GCP_BRONZE_BUCKET=velib-bronze-velib-analytics-xxxxx
GCP_BIGQUERY_SILVER_DATASET=velib_silver
GCP_BIGQUERY_GOLD_DATASET=velib_gold
GCP_SERVICE_ACCOUNT_KEY=/path/to/velib-pipeline-sa.json
PIPELINE_TARGET=cloud
```

## Makefile targets to add

```makefile
cloud-init:           # terraform init
cloud-plan:           # terraform plan with cost preview
cloud-up:             # terraform apply
cloud-down:           # terraform destroy (with confirmation)
cloud-deploy-spark:   # upload the Spark job to GCS
cloud-run-ingestion:  # trigger an Airflow DAG in cloud mode
cloud-dbt-run:        # dbt run with bigquery_cloud profile
cloud-dbt-test:       # dbt test with bigquery_cloud profile
cloud-logs:           # tail Dataproc logs of the last batch
cloud-cost:           # current month cost report
```

## Cost guardrails

- GCP budget set at 30 EUR per month with alerts at 50%, 80%, 100%
- GCS lifecycle rules automatically delete data older than one year
- BigQuery: partitioning mandatory, clustering enabled, no `SELECT *` in dbt models
- Dataproc Serverless only (no permanent cluster)
- `make cloud-down` at the end of a dev session to leave only storage running

## Technical watch points

1. **Parquet on GCS**: verify that the schema written by the local DAG is readable by Spark on Dataproc (encoding, compression)
2. **dbt macros**: some Postgres functions (`DATE_TRUNC`, `INTERVAL`) have different equivalents in BigQuery
3. **Time zones**: BigQuery stores in UTC by default, partitioning must stay consistent with GCS partitions
4. **Quotas**: Dataproc Serverless has a default quota of 60 batches per day per project, enough but worth monitoring
5. **IAM propagation**: permissions sometimes take several minutes to propagate after a `terraform apply`

## Definition of done

The migration is considered complete when:

- [ ] `make cloud-up` provisions the full infrastructure in under 10 minutes
- [ ] `make cloud-run-ingestion` writes correctly to GCS
- [ ] The Dataproc batch reads Bronze from GCS and writes Silver to BigQuery
- [ ] `make cloud-dbt-run` produces Gold models in BigQuery without error
- [ ] dbt tests pass on both profiles (local and cloud)
- [ ] A Looker Studio dashboard shows at least the three KPIs from the README
- [ ] `make cloud-down` destroys the infrastructure cleanly
- [ ] Cumulative test cost stays under 10 EUR
- [ ] The GitHub README clearly exposes a "Cloud deployment" section
