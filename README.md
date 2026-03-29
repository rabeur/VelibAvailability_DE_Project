# 🚴 Vélib' Data Pipeline - Paris Bicycle Sharing Analytics

This project was built as part of the [Data Engineering Zoomcamp 2026][zoomcamp_website_link] by **DataTalks Club**.

## 📊 Problem Definition

The objective is to build a local, production-like data platform around **Vélib' open data** and transform raw station snapshots into analytics-ready datasets.

The pipeline supports spatio-temporal analysis of bike availability to identify areas under pressure and prepare data for forecasting models.

## 🎯 Main Objectives

- [x] Ingest real-time Vélib data every minute
- [x] Store raw snapshots in a Bronze data lake (Parquet, partitioned)
- [x] Transform Bronze data into a Silver warehouse model (Spark + PostgreSQL)
- [x] Run automated data quality checks and persist reports
- [x] Add dbt transformation layer for Gold business models
- [x] Add a dashboarding layer with Apache Superset

## ✅ Current Progress

### Phase 1: Infrastructure ✅ Completed
- [x] Docker Compose stack: PostgreSQL, Spark, Jupyter, Airflow
- [x] Local-first environment with reproducible setup via Makefile
- [x] SQL initialization for Silver and Gold schemas
- [x] Superset service with dedicated metadata database (`superset_meta`)

### Phase 2: Ingestion ✅ Completed
- [x] Airflow ingestion DAG running every minute
- [x] Stable Bronze schema with typed columns
- [x] Geo flattening (`coordonnees_geo` → `lat`/`lon`)
- [x] Partitioned writes: `data_lake/bronze/velib/ingestion_date=YYYY-MM-DD/hour=HH/`

### Phase 3: Data Quality ✅ Implemented
- [x] Dedicated Airflow data-quality DAG (runs every minute)
- [x] Quality checks: schema, nulls, duplicates, freshness, ranges, consistency
- [x] Partitioned text reports in `data_lake/reports/data_quality/report_date=YYYY-MM-DD/hour=HH/`

### Phase 4: Bronze → Silver ✅ Implemented (core)
- [x] Hourly Airflow Bronze cleanup DAG (corrupted + duplicate parquet handling)
- [x] Trigger-based Bronze-to-Silver DAG execution after cleanup
- [x] Spark transformation job with data cleaning, normalization, and metrics
- [x] PostgreSQL Silver loading and validation
- [x] Runtime summary task with station/snapshot monitoring

### Phase 5: Gold Analytics ✅ Implemented
- [x] dbt Gold models (dimensions, facts, marts)
- [x] Airflow dbt DAG (`velib_dbt_gold_transformation`)
- [x] dbt run/test workflow from Makefile

### Phase 6: Visualization ✅ Implemented (base)
- [x] Apache Superset service integrated in Docker Compose
- [x] Superset initialization flow (metadata DB + admin bootstrap)
- [x] PostgreSQL connectivity ready for Gold dashboarding

## 🆕 Recent Evolution Added to the Project

- Airflow now triggers the Spark job from the scheduler container using the Docker SDK.
- Docker socket permission handling was added via `DOCKER_GID` and `group_add` in Airflow services.
- Silver validation and summary tasks use direct PostgreSQL connections (no `docker exec` dependency).
- Data-quality reports were moved to partitioned paths aligned with data-lake conventions.
- Bronze hourly cleanup now removes corrupted Parquet files and minute-level duplicates before Silver transformation.
- Code style tooling was added (`ruff`, `black`, `sqlfluff`) with Makefile targets.

## 🏗️ Architecture

![Architecture Diagram](docs/diagrams/architecture_diagram.png)
(Architecture diagram created with [Lucidchart][lucidchart_website_link].)

## 🛠️ Tech Stack

| Component | Technology | Role |
|-----------|------------|------|
| Orchestration | **Apache Airflow 2.9** | Scheduling, dependency management, and pipeline monitoring |
| Ingestion | **Python (requests + pandas)** | Real-time API extraction, schema stabilization, Bronze writes |
| Processing | **Apache Spark 3.5** | Bronze-to-Silver transformations and enrichment |
| Data Warehouse | **PostgreSQL 17** | Silver analytical storage |
| Data Lake | **Local filesystem + Parquet** | Raw snapshot storage with partitioning |
| Interactive Analysis | **Jupyter PySpark Notebook** | Exploration and profiling |
| Infrastructure | **Docker + Docker Compose** | Reproducible local deployment |
| Transformation | **dbt** | Gold models, tests, and lineage |
| Visualization | **Apache Superset** | BI dashboards on PostgreSQL Gold models |

## 🧭 Development Methodology

To keep all components consistent (Airflow DAGs, scripts, Spark jobs, dbt SQL), the project uses one shared set of conventions:

- Python style: `ruff` + `black` settings from [pyproject.toml](pyproject.toml)
- SQL style: `sqlfluff` settings from [.sqlfluff](.sqlfluff)
- Naming:
  - snake_case for Python variables/functions
  - explicit task names in DAGs (`load_data`, `quality_checks`, etc.)
  - dbt layer naming kept as `stg_`, `dim_`, `fact_`, `mart_`
- Comments:
  - explain why (decision, constraint, edge case), not obvious line-by-line actions
  - keep comments concise and in English across pipeline code

### Quality Commands

Use these targets before merging changes:

```bash
make format-python
make lint-python
make format-sql
make lint-sql
```

## 📁 Pipeline Components

### Airflow DAGs
- `velib_ingestion_pipeline` (every minute)
  - API extraction, schema enforcement, Bronze write, basic validation
- `velib_data_quality` (every minute)
  - Snapshot quality checks and partitioned report generation
- `velib_bronze_cleanup_hourly` (hourly)
  - Checks previous-hour Bronze partition
  - Removes corrupted parquet and minute-level duplicates
  - Triggers Silver transformation DAG
- `velib_silver_transformation_hourly` (trigger-based)
  - Bronze availability check, Spark execution, Silver validation, summary
- `velib_dbt_gold_transformation` (daily 03:00)
  - dbt deps → staging → gold → tests

### Spark Job
- `spark_jobs/bronze_to_silver.py`
  - Cleans and normalizes fields
  - Computes operational metrics (`occupancy_rate`, `availability_rate`, `service_rate`)
  - Loads stations and availability into Silver tables

## 📦 Data Model

### Bronze Layer (Parquet)
Partitioned snapshots:
- `ingestion_date`
- `hour`

Main fields include:
- `stationcode`, `name`, `capacity`
- `numdocksavailable`, `numbikesavailable`, `mechanical`, `ebike`
- `is_installed`, `is_renting`, `is_returning`
- `lat`, `lon`
- `nom_arrondissement_communes`, `code_insee_commune`
- `ingestion_timestamp`, `snapshot_id`

### Silver Layer (PostgreSQL)
- `silver.stations` (station dimension)
- `silver.station_availability` (hourly/time-based facts)

### Gold Layer (dbt)
- `gold.dim_stations`
- `gold.fact_hourly_availability`
- `gold.fact_daily_station_stats`
- `gold.mart_station_performance`
- `gold.mart_peak_hours_by_district`

## 🚀 Getting Started

### Prerequisites
- Docker Desktop or Docker Engine
- Linux/WSL terminal
- `make`
- `python3` (recommended for local lint/format tooling)

### Installation

```bash
# 1) Clone project
git clone https://github.com/rabeur/VelibAvailability_DE_Project.git
cd VelibAvailability_DE_Project

# 2) Bootstrap env + build + up
make first-launch

# 3) (Optional) check service status
make status
```

### Service Endpoints
- Airflow: `http://localhost:8081`
- Spark Master UI: `http://localhost:8080`
- Jupyter: `http://localhost:8888`
- Superset: `http://localhost:8088`
- PostgreSQL: `localhost:5432`
- pgAdmin: `http://localhost:5050`

## 🧪 Data Quality Reports

Reports are generated as text files in:

`data_lake/reports/data_quality/report_date=YYYY-MM-DD/hour=HH/report-YYYY-MM-DD-HH-mm.txt`

Each report contains:
- rows checked
- tests passed / failed
- success rate
- detailed PASS/FAIL messages per check

## 🔍 Example Analyses Enabled

- Bike availability trend by hour/day
- Station occupancy stress zones
- Empty/full station rate by time bucket
- Data freshness and ingestion reliability monitoring

## 📚 Key Challenges Solved

- Building historical snapshots from real-time API data
- Stabilizing schema across rapidly ingested Parquet files
- Enforcing data quality gates before downstream analytics
- Running Spark jobs from Airflow in Dockerized local environment


## 🔮 Next Steps

- [ ] Add Superset datasets/charts export (versioned dashboard-as-code)
- [ ] Add semantic KPI dictionary (business glossary)
- [ ] Add alerting channels (email/Slack) for pipeline + data-quality incidents
- [ ] Add CI pipeline for `ruff` + `sqlfluff` + dbt tests
- [ ] Prepare cloud migration path (GCP/AWS)

## 🌍 Local-first, Cloud-ready

- Local filesystem ↔ object storage (S3/GCS/ADLS)
- PostgreSQL ↔ analytical warehouse (BigQuery/Snowflake)
- Spark local ↔ scalable distributed processing
- Airflow local ↔ managed orchestration services

[zoomcamp_website_link]: https://github.com/DataTalksClub/data-engineering-zoomcamp
[lucidchart_website_link]: https://www.lucidchart.com/pages
[docker_desktop]: https://www.docker.com/products/docker-desktop