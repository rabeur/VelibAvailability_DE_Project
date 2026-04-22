# GCP cost management

Project budget target: under 20 EUR per month. This document lists the
cost drivers, the guardrails in place, and the commands to monitor spend.

## Target per category

| Category | Expected usage | Estimated monthly cost |
|----------|----------------|-----------------------|
| GCS Bronze | 5 to 20 GB of Parquet snapshots | < 0.50 EUR |
| BigQuery storage | 2 to 5 GB Silver + Gold | < 0.20 EUR |
| BigQuery query | < 100 GB scanned / month | 0 to 0.50 EUR |
| Dataproc Serverless | 2 to 10 batches / day, ~3 min each | 2 to 10 EUR |
| Transfers and logs | Standard monitoring | < 1 EUR |
| **Indicative total** | | **3 to 12 EUR / month** |

The dominant cost is Dataproc Serverless. A misconfigured batch (too many executors, looping runtime) can blow the budget quickly: that is where the main guardrails focus.

## Guardrails in place

### Budget and alerts

A 20 EUR budget with 50 / 80 / 100 % alerts must be configured from the first `terraform apply` (see `setup.md`). The 80 % alert mentally caps non-critical runs for the rest of the month.

### GCS: automatic lifecycle rules

The Bronze bucket (`cloud/terraform/gcs.tf`) automatically tiers objects:

- Standard for 30 days (hot, fast read for debugging)
- Nearline between 30 and 90 days (2x cheaper storage)
- Coldline between 90 and 365 days (4x cheaper)
- Deletion after 365 days

No versioning: snapshots are append-only and can be re-ingested from the Velib API if needed.

### BigQuery: mandatory partitioning and clustering

dbt rule (see root `CLAUDE.md`): never `SELECT *` in a model targeting BigQuery. Every unused column is charged at read time.

Silver tables are partitioned by day on `ingestion_timestamp` and clustered on `stationcode`. A typical one-day filter reads about 15 MB instead of 2 GB.

### Dataproc Serverless only

No permanent cluster. Default batch configuration:

- 2 executors, 4 GB memory each
- Runtime 2.2 LTS
- No NAT gateway, no custom VPC (would cost more than the batch itself)

A standard batch runs 2 to 4 minutes and costs around 0.10 to 0.30 EUR.

### Session hygiene

Run `make cloud-down` at the end of a dev session. Worth remembering: empty BigQuery datasets and a near-empty bucket cost only a few cents, but a forgotten Dataproc batch that loops can cost several EUR.

## Useful commands

### Current month spend

```bash
gcloud billing accounts list
# note the billing account ID

gcloud alpha billing accounts describe <BILLING_ACCOUNT_ID>
```

For per-SKU breakdown, use the GCP console: `Billing > Reports`, filtered on the `velib-analytics-*` project.

### Bucket storage volume

```bash
gsutil du -sh gs://$(terraform output -raw bronze_bucket_name)
```

### Volume scanned by BigQuery queries

For any ad-hoc query, run a dry run first:

```bash
bq query --dry_run --use_legacy_sql=false "SELECT ... FROM velib_silver.stations_snapshot WHERE ..."
```

The dry run returns the number of bytes that would be scanned. Useful to detect a missing partition filter.

### Dataproc batch cost

```bash
gcloud dataproc batches list --region=europe-west1 --limit=10
gcloud dataproc batches describe <batch-id> --region=europe-west1
```

The `runtimeInfo.approximateUsage` field returns the DCU (Dataproc Compute Units) consumed. Multiply by the regional DCU/hour price.

## Useful free tier

- GCS: 5 GB of Standard storage free per month in `us-*`. In `europe-west1`, no free storage tier but low unit prices.
- BigQuery: 10 GB storage and 1 TB query free per month, across all projects.
- Cloud Logging: 50 GB of ingested logs free per project.

Do not move the project to `us-*` just for the GCS free tier: cross-region transfer costs and latency against the Velib source would cancel the savings.

## Drift signals

Trigger an audit when:

- The billing dashboard crosses 15 EUR before the 25th of the month
- A Dataproc batch exceeds 10 minutes (likely a bug or unexpected volume)
- `gsutil du` reports more than 50 GB on the Bronze bucket (check lifecycle rules)
- An ad-hoc BigQuery query scans more than 10 GB (missing partition filter)

## Explicitly forbidden on this project

- Permanent Dataproc cluster
- Cloud SQL
- Managed Cloud Composer (Airflow stays local)
- NAT Gateway, Cloud Interconnect
- Any private endpoint or VPC peering
- Any resource created outside `cloud/terraform/`
