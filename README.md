# 🚴 Vélib' Data Platform

**End-to-end data engineering pipeline for Paris' bike sharing network — from real-time API ingestion to analytics-ready dashboards.**

[![Airflow](https://img.shields.io/badge/orchestration-Airflow-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Spark](https://img.shields.io/badge/processing-Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/transform-dbt-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/warehouse-PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Superset](https://img.shields.io/badge/bi-Superset-20A6C9?logo=apachesuperset&logoColor=white)](https://superset.apache.org/)
[![Docker](https://img.shields.io/badge/infra-Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

---

## Overview

A production-grade, local-first data platform that ingests real-time Vélib' open data every minute, lands it in a partitioned data lake, transforms it through Bronze → Silver → Gold layers with Spark and dbt, runs automated quality checks, and serves analytical models for operational dashboarding in Superset.

The project was built as the capstone of the [Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp) by DataTalks Club.

> **Capability snapshot:** [N] Vélib' stations tracked · [N]k snapshots ingested per day · [N] dbt models · [N] GB data lake · hourly Bronze → Silver → Gold refresh

---

## Screenshots

> *Placeholder — dashboards, DAGs and lineage screenshots to be added.*

| | |
|---|---|
| **Airflow DAGs** | ![Airflow DAGs placeholder](docs/screenshots/airflow-dags.png) |
| **dbt lineage** | ![dbt lineage placeholder](docs/screenshots/dbt-lineage.png) |
| **Superset dashboard** | ![Superset dashboard placeholder](docs/screenshots/superset-dashboard.png) |

---

## Architecture

![Architecture Diagram](docs/diagrams/architecture_diagram.png)

The pipeline follows the medallion architecture pattern with clear separation of concerns:

**Bronze layer** captures raw API snapshots as partitioned Parquet files on local disk, with stable schema and geo flattening applied at ingestion time. Partitioning by `ingestion_date` and `hour` keeps downstream reads fast and enables incremental processing.

**Silver layer** is materialized in PostgreSQL after a Spark job reads the previous hour's Bronze partition, applies data cleaning and normalization, computes operational metrics (occupancy rate, availability rate, service rate), and enriches Paris stations with their administrative district using official GeoJSON boundaries.

**Gold layer** is built with dbt on top of Silver. It exposes dimensional models, hourly and daily facts, and business-oriented marts designed to power Superset dashboards focused on real-time operational monitoring.

---

## Tech stack

| Layer | Tool | Role |
|---|---|---|
| Orchestration | Apache Airflow | Scheduling, dependency management, trigger-based DAG chaining |
| Ingestion | Python, Requests, Pandas | Real-time API extraction, schema stabilization, Bronze writes |
| Processing | Apache Spark | Distributed Bronze to Silver transformations and enrichment |
| Warehouse | PostgreSQL | Silver analytical storage |
| Transformation | dbt | Gold models, testing, lineage documentation |
| Data lake | Local filesystem, Parquet | Raw snapshot storage with date and hour partitioning |
| Infrastructure | Docker, Docker Compose | Reproducible local deployment |
| Visualization | Apache Superset | BI dashboards on Gold models |
| Code quality | ruff, black, sqlfluff | Consistent style across Python and SQL |

---

## Pipeline components

The platform runs five Airflow DAGs, each with a single, well-defined responsibility.

| DAG | Frequency | Purpose |
|---|---|---|
| `velib_ingestion_pipeline` | every minute | API extraction, schema enforcement, Bronze write, basic validation |
| `velib_data_quality` | every minute | Schema, null, duplicate, freshness, range and consistency checks with persisted reports |
| `velib_bronze_cleanup_hourly` | hourly | Removes corrupted Parquet files and minute-level duplicates from the previous hour partition |
| `velib_silver_transformation_hourly` | trigger-based | Executes the Spark job, loads Silver tables, validates the output |
| `velib_dbt_gold_transformation` | daily at 03:00 | Runs dbt deps, staging, gold and tests |

The Spark job itself (`spark_jobs/bronze_to_silver.py`) handles field cleaning, normalization, the three operational metrics and the loading of both the station dimension and the availability facts into PostgreSQL.

---

## Data model

### Bronze — Parquet partitions

Partitioned by `ingestion_date` and `hour`, with stable schema across snapshots. Key fields include station identity (`stationcode`, `name`, `capacity`), real-time availability (`numdocksavailable`, `numbikesavailable`, `mechanical`, `ebike`), operational status (`is_installed`, `is_renting`, `is_returning`), geography (`lat`, `lon`, `nom_arrondissement_communes`) and lineage markers (`ingestion_timestamp`, `snapshot_id`).

### Silver — PostgreSQL

- `silver.stations` — station dimension with Paris district enrichment
- `silver.station_availability` — time-based availability facts

### Gold — dbt models

- `gold.dim_stations`
- `gold.fact_hourly_availability`
- `gold.fact_daily_station_stats`
- `gold.mart_station_performance`
- `gold.mart_peak_hours_by_district`

---

## Notable engineering decisions

**Historical snapshots from a real-time API.** The Vélib' API only exposes the current state of the network. Building historical analytics required designing a reliable minute-level ingestion loop, a stable Bronze schema that survives upstream API changes, and a lineage marker (`snapshot_id`, `ingestion_timestamp`) on every row to support deduplication and replay.

**Schema stability across rapid Parquet writes.** Writing Parquet every minute under schema drift is a known failure mode. The ingestion DAG enforces a typed column contract and flattens the nested `coordonnees_geo` field at write time, so downstream Spark and dbt reads never hit mixed schemas.

**Trigger-based DAG orchestration instead of time-based coupling.** The Silver transformation is not scheduled directly — it is triggered only after the Bronze cleanup confirms the previous hour's partition is clean. This eliminates a whole class of race conditions between writers and readers.

**Running Spark from Airflow inside Docker.** The Airflow scheduler uses the Docker SDK to spawn the Spark job container, avoiding the brittleness of `docker exec` from inside a DAG. Silver validation and summary tasks hit PostgreSQL directly through standard connections.

**District-level enrichment for operational analytics.** Paris stations are mapped to their official arrondissement by joining their coordinates against the municipal GeoJSON boundary file. This turns geographic coordinates into a business-meaningful dimension that powers the `mart_peak_hours_by_district` analytics.

**Data quality as a first-class citizen.** Quality is not a post-hoc check but a dedicated DAG running at ingestion frequency, with partitioned text reports stored in `data_lake/reports/data_quality/`. Every report records rows checked, tests passed and failed, and a per-check PASS or FAIL message.

---

## Getting started

### Prerequisites

- Docker Desktop or Docker Engine
- A Linux or WSL terminal
- `make`
- `python3` (recommended for the local linting and formatting tooling)

### Bootstrap

```bash
git clone https://github.com/rabeur/VelibAvailability_DE_Project.git
cd VelibAvailability_DE_Project

make first-launch      # build, up, init database, init Superset
make status            # check service health
```

### Service endpoints

| Service | URL |
|---|---|
| Airflow | http://localhost:8081 |
| Spark Master UI | http://localhost:8080 |
| Superset | http://localhost:8088 |
| pgAdmin | http://localhost:5050 |
| PostgreSQL | localhost:5432 |

### Code quality

```bash
make format-python
make lint-python
make format-sql
make lint-sql
```

---

## Example analyses enabled

- Bike availability trends by hour and day
- Station occupancy stress zones
- Empty and full station rates by time bucket
- Data freshness and ingestion reliability monitoring
- Paris operational analysis by arrondissement

### Superset dashboard example

| Chart | Dataset | Metric | Business value |
|---|---|---|---|
| Network pressure over the last few hours | `gold.fact_hourly_availability` | `AVG(avg_occupancy_rate)` | Real-time pressure on the network |
| Top 10 stations most often empty | `gold.mart_station_performance` | `AVG(avg_pct_time_empty)` | Highlights the most critical stations for riders |
| Peak pressure hours by arrondissement | `gold.mart_peak_hours_by_district` | `AVG(avg_occupancy_rate)` | Identifies when and where districts are under stress |

---

## Roadmap

The local platform is complete and operational. The following extensions are actively planned.

### In progress — Cloud deployment on Google Cloud Platform

A parallel cloud branch is being built to demonstrate portability and cloud-readiness, while preserving the local platform intact. The hybrid design keeps Airflow local to avoid the fixed cost of a managed scheduler and delegates storage, processing and warehousing to managed GCP services.

| Component | Local | GCP target |
|---|---|---|
| Data lake | Filesystem, Parquet | Google Cloud Storage |
| Processing | Spark on Docker | Dataproc Serverless |
| Warehouse | PostgreSQL | BigQuery |
| Transform | dbt-postgres | dbt-bigquery |
| Viz | Superset | Looker Studio |
| IaC | Docker Compose | Terraform |

### Planned

- CI pipeline for `ruff`, `sqlfluff` and dbt tests
- Alerting channels (email and Slack) for pipeline incidents
- KPI dictionary and business glossary
- Export of Superset datasets and charts as code

---

## Repository structure

```
VelibAvailability_DE_Project/
├── airflow/dags/              # Airflow DAGs (ingestion, quality, cleanup, Silver, Gold)
├── spark_jobs/                # Spark transformation jobs
├── dbt/                       # dbt project (staging, gold, tests)
├── data_lake/                 # Bronze Parquet partitions and quality reports
├── scripts/                   # Utility scripts (arrondissement enrichment, bootstrap)
├── docker-compose.yml
├── Makefile
└── README.md
```

---

## About

Built by **Adrien Rabier**, Data Engineer based in Paris.
4 years in software engineering on mission-critical industrial systems (RATP, SNCF, energy), specialized in SQL, data modeling, batch migrations and Python automation.

[LinkedIn](https://www.linkedin.com/in/adrien-rabier-9a699a150/) · [GitHub](https://github.com/rabeur) · rabiera69@gmail.com