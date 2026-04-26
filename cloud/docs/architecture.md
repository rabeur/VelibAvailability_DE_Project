# Cloud architecture diagram

Target deployment of the Velib pipeline on Google Cloud Platform. Airflow
stays local; all storage, processing and warehousing happens on GCP. The
same `spark_jobs/` and `dbt/` sources power both the local and cloud
targets, selected at runtime by `PIPELINE_TARGET`.

```mermaid
flowchart LR
    subgraph Local["Local host (Docker Compose)"]
        AF[Airflow 2.9<br/>scheduler + webserver]
        DBT[dbt container<br/>dual adapter<br/>postgres + bigquery]
    end

    subgraph GCP["Google Cloud Platform (europe-west1)"]
        API[Velib Open API]
        GCS[("GCS<br/>velib-bronze-*<br/>Bronze Parquet")]
        DP{{Dataproc Serverless<br/>2x4GB executors<br/>per-second billing}}
        BQS[("BigQuery<br/>velib_silver<br/>partitioned + clustered")]
        BQG[("BigQuery<br/>velib_gold<br/>dbt marts")]
        LS[Looker Studio<br/>public dashboards]
    end

    API -->|HTTPS every 1 min| AF
    AF -->|"ingestion DAG<br/>GCSHook.upload()"| GCS
    AF -->|"bronze-to-silver DAG<br/>DataprocCreateBatchOperator"| DP
    DP -->|read Parquet| GCS
    DP -->|write partitioned| BQS
    AF -->|"gold DAG<br/>docker exec dbt"| DBT
    DBT -->|"bigquery_cloud target<br/>service-account JSON"| BQS
    DBT -->|create tables| BQG
    BQG --> LS

    classDef gcp fill:#e8f0fe,stroke:#4285F4,stroke-width:1px,color:#1f1f1f
    classDef local fill:#f6f6f6,stroke:#616161,stroke-width:1px,color:#1f1f1f
    class GCS,DP,BQS,BQG,LS,API gcp
    class AF,DBT local
```

## Data contract between layers

| Layer | Location (cloud mode) | Format | Partition | Clustering |
|-------|-----------------------|--------|-----------|------------|
| Bronze | `gs://velib-bronze-<proj>/bronze/velib/` | Parquet (snappy) | `ingestion_date=`, `hour=` | n/a |
| Silver | `<proj>.velib_silver.station_availability` | BigQuery native | day on `ingestion_timestamp` | `stationcode` |
| Silver | `<proj>.velib_silver.stations` | BigQuery native | none (dim) | `stationcode` |
| Gold | `<proj>.velib_gold.*` | BigQuery native, dbt-managed | model-specific | model-specific |

## Control plane

Airflow triggers every job; no Composer and no persistent Dataproc
cluster. Dataproc Serverless boots a batch on demand (60 to 90 s startup)
and shuts down immediately after, keeping the monthly bill within the
20 EUR budget target (see `cost_management.md`).

## IAM

A single `velib-pipeline-sa` service account acts for Airflow, Dataproc
jobs and dbt. Key JSON is mounted into the local containers via
`docker-compose.yml`, never committed. Terraform owns the role bindings
(`storage.objectAdmin` on the Bronze bucket, `bigquery.dataEditor` on
both datasets, `dataproc.editor` + `dataproc.worker` for batch
submission and execution).

## Relationship with the local diagram

`docs/diagrams/local_architecture_diagram.png` at the repository root shows the
same flow with Postgres + Superset in place of BigQuery + Looker Studio.
The two diagrams are intentionally isomorphic so the dual-mode DAGs and
Spark jobs can be audited in parallel.
