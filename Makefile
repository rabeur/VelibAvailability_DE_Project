# Load .env variable if .env file exists
ifneq (,$(wildcard .env))
    include .env
    export $(shell sed 's/=.*//' .env)
endif

# .env.cloud is intentionally NOT auto-included: sourcing it globally
# leaks PIPELINE_TARGET=cloud and GCP_* into every local target (make up,
# make status, make dbt-run), silently switching the stack to cloud mode.
# Cloud-* recipes source it explicitly at recipe time via $(CLOUD_ENV).

# Variables
COMPOSE_FILE = docker-compose.yml
PROJECT_NAME = velib_project
ENV_FILE = .env
PYTHON = python3
PARQUET_CLEANUP_IMAGE = velib-cleanup
PARQUET_CLEANUP_ROOT = $(PWD)/data_lake/bronze/velib
INGEST_IMAGE = velib-ingest
INGEST_DATA_LAKE_ROOT = $(PWD)/data_lake
CLOUD_TF_DIR = cloud/terraform
CLOUD_SCRIPTS_DIR = cloud/scripts

# Source .env.cloud into a single sub-shell for one recipe line. Used by
# every cloud-* target that needs PIPELINE_TARGET=cloud or GCP_* at runtime
# without polluting the local targets.
CLOUD_ENV = set -a && . ./.env.cloud && set +a

.PHONY: build up up-logs down logs clean first-launch status restart shell fix-perms give-perms \
	dbt-deps dbt-run dbt-run-staging dbt-run-gold dbt-test dbt-all dbt-docs \
	silver-schema gold-schema superset-db cleanup-parquet-build cleanup-parquet \
	cleanup-parquet-delete ingest-build ingest-manual format-python lint-python \
	format-sql lint-sql help \
	cloud-init cloud-stack-up cloud-plan cloud-up cloud-down cloud-deploy-spark \
	cloud-run-ingestion cloud-dbt-run cloud-dbt-test cloud-logs cloud-cost \
	cloud-backfill cloud-backfill-apply \
	_require-env-cloud _require-tfvars

# Build docker images
build:
	docker compose -f $(COMPOSE_FILE) build

# Launch services in detached mode
up:
	docker compose -f $(COMPOSE_FILE) up -d

# Launch services with logs for debugging
up-logs:
	docker compose -f $(COMPOSE_FILE) up

# Stop services
down:
	docker compose -f $(COMPOSE_FILE) down

# Show logs of all services
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# Cleanse all containers, volumes, and images
clean:
	docker compose -f $(COMPOSE_FILE) down -v --rmi all
	docker system prune -f

# First launch: build and up, with instructions
first-launch:
	@echo "Initial config..."
	@if [ ! -f $(ENV_FILE) ]; then \
		echo ".env make file..."; \
		echo "POSTGRES_USER=velib" > $(ENV_FILE); \
		echo "POSTGRES_PASSWORD=velib" >> $(ENV_FILE); \
		echo "POSTGRES_DB=velib_dw" >> $(ENV_FILE); \
		echo "PGADMIN_PASSWORD=admin" >> $(ENV_FILE); \
		echo "PGADMIN_EMAIL=admin@velib.com" >> $(ENV_FILE); \
		echo "AIRFLOW_ADMIN_USERNAME=admin" >> $(ENV_FILE); \
		echo "AIRFLOW_ADMIN_PASSWORD=admin" >> $(ENV_FILE); \
		echo "AIRFLOW_FERNET_KEY=$$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")" >> $(ENV_FILE); \
		echo "AIRFLOW_UID=50000" >> $(ENV_FILE); \
		echo "AIRFLOW_GID=0" >> $(ENV_FILE); \
		echo "DOCKER_GID=1001" >> $(ENV_FILE); \
		echo "SUPERSET_SECRET_KEY=$$(python3 -c 'import secrets; print(secrets.token_hex(32))')" >> $(ENV_FILE); \
		echo "SUPERSET_ADMIN_PASSWORD=admin" >> $(ENV_FILE); \
	else \
		echo ".env already exist."; \
	fi
	@echo "Airflow folders permissions correction..."
	@if command -v sudo >/dev/null 2>&1; then \
	    sudo chown -R 50000:0 airflow || echo "Airflow permissions not corrected (sudo required?)."; \
	    sudo chmod -R 775 airflow || true; \
	else \
	    chown -R 50000:0 airflow || echo "Airflow permissions not corrected (chown required)."; \
	    chmod -R 775 airflow || true; \
	fi
	@echo "data_lake's permissions correction..."
	@if command -v sudo >/dev/null 2>&1; then \
		sudo chown -R 50000:0 data_lake 2>/dev/null || echo "Permissions non corrected (sudo required or folder don't exists)."; \
	else \
		chown -R 50000:0 data_lake 2>/dev/null || echo "Permissions non corrected (chown required)."; \
	fi
	@echo "Build and launch services..."
	$(MAKE) build
	$(MAKE) up
	@echo "Waiting for postgres to be ready before creating Superset database..."
	$(MAKE) superset-db
	@echo "First launch finished. Access the following services:"
	@echo "- Airflow  : http://localhost:8081"
	@echo "- Spark    : http://localhost:8080"
	@echo "- Superset : http://localhost:8088 (user: admin / password: see SUPERSET_ADMIN_PASSWORD in .env)"
	@echo "- PostgreSQL : localhost:5432"

# Check status of containers
status:
	docker compose -f $(COMPOSE_FILE) ps

# Restart specific service (ex: make restart SERVICE=airflow-webserver)
restart:
	docker compose -f $(COMPOSE_FILE) restart $(SERVICE)

# Bash acces to a container (ex: make shell SERVICE=postgres)
shell:
	docker compose -f $(COMPOSE_FILE) exec $(SERVICE) bash

# Give required permission to airflow and datalake folder in order to make docker-compose work (required in prod)
fix-perms:
	sudo chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} airflow
	sudo chmod -R 775 airflow
	sudo chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} data_lake

#Give permission to modify airflow and datalake folder  (usefull in devmode but useless in prod)
give-perms:
	sudo chown -R ${USER}:${USER} airflow
	sudo chown -R ${USER}:${USER} data_lake

# ─── dbt / Gold layer ────────────────────────────────────────────────────────

# Install dbt package dependencies (dbt-utils etc.)
dbt-deps:
	docker exec velib_dbt dbt deps --profiles-dir /usr/app/dbt

# Run all dbt models (staging + gold)
dbt-run:
	docker exec velib_dbt dbt run --profiles-dir /usr/app/dbt

# Run only staging views
dbt-run-staging:
	docker exec velib_dbt dbt run --profiles-dir /usr/app/dbt --select staging

# Run only Gold models
dbt-run-gold:
	docker exec velib_dbt dbt run --profiles-dir /usr/app/dbt --select gold

# Run dbt data-quality tests
dbt-test:
	docker exec velib_dbt dbt test --profiles-dir /usr/app/dbt

# Full dbt pipeline: deps → run → test
dbt-all: dbt-deps dbt-run dbt-test

# Generate and serve dbt docs locally on port 8082
dbt-docs:
	docker exec velib_dbt dbt docs generate --profiles-dir /usr/app/dbt
	docker exec -d -p 8082:8080 velib_dbt dbt docs serve --profiles-dir /usr/app/dbt --port 8080
	@echo "dbt docs available at http://localhost:8082"

# Create the silver schema manually (needed if the DB already existed before adding this schema)
silver-schema:
	docker exec -i velib_postgres psql -U velib -d velib_dw < sql/02_init_silver_schema.sql

# Create the gold schema manually (needed if the DB already existed before adding this schema)
gold-schema:
	docker exec -i velib_postgres psql -U velib -d velib_dw < sql/03_init_gold_schema.sql

# Create the superset_meta database on postgres (idempotent — safe to re-run)
superset-db:
	@echo "Creating superset_meta database if not exists..."
	-docker exec velib_postgres psql -U $(POSTGRES_USER) -c "CREATE DATABASE superset_meta;" 2>/dev/null || true
	@echo "superset_meta database is ready."

# Format Python files with Ruff formatter
format-python:
	$(PYTHON) -m ruff format airflow scripts spark_jobs superset

# Lint Python files with Ruff
lint-python:
	$(PYTHON) -m ruff check airflow scripts spark_jobs superset

# Auto-fix and format SQL files with SQLFluff
format-sql:
	sqlfluff fix sql dbt/velib_dbt/models dbt/velib_dbt/macros --force

# Lint SQL files with SQLFluff
lint-sql:
	sqlfluff lint sql dbt/velib_dbt/models dbt/velib_dbt/macros

# Build image for parquet cleanup script
cleanup-parquet-build:
	docker build -f scripts/cleanup_parquet/Dockerfile.cleanup_parquet -t $(PARQUET_CLEANUP_IMAGE) .

# Dry-run parquet cleanup (scan only)
cleanup-parquet: cleanup-parquet-build
	@echo "Scanning parquet files under: $(PARQUET_CLEANUP_ROOT)"
	docker run --rm \
		-v "$(PARQUET_CLEANUP_ROOT):/data" \
		$(PARQUET_CLEANUP_IMAGE) --root /data

# Delete corrupted + duplicate parquet files
cleanup-parquet-delete: cleanup-parquet-build
	@echo "Deleting corrupted/duplicate parquet files under: $(PARQUET_CLEANUP_ROOT)"
	docker run --rm \
		-v "$(PARQUET_CLEANUP_ROOT):/data" \
		$(PARQUET_CLEANUP_IMAGE) --root /data --delete

# Build image for manual ingestion script
ingest-build:
	docker build -f scripts/ingestion/Dockerfile.ingest_velib -t $(INGEST_IMAGE) .

# Run ingestion once manually
ingest-manual: ingest-build
	@echo "Running manual ingestion and writing outputs under: $(INGEST_DATA_LAKE_ROOT)"
	docker run --rm \
		-v "$(INGEST_DATA_LAKE_ROOT):/app/data_lake" \
		$(INGEST_IMAGE)

# ─── Cloud (GCP) deployment ──────────────────────────────────────────────────

# Guards: commands that hit GCP need .env.cloud and terraform.tfvars. We fail
# loudly rather than silently running with defaults that could bill the wrong
# project or create unintended resources.
_require-env-cloud:
	@if [ ! -f .env.cloud ]; then \
		echo "ERROR: .env.cloud is missing. Copy .env.cloud.example and fill it in."; \
		exit 1; \
	fi

_require-tfvars:
	@if [ ! -f $(CLOUD_TF_DIR)/terraform.tfvars ]; then \
		echo "ERROR: $(CLOUD_TF_DIR)/terraform.tfvars is missing."; \
		echo "       Copy $(CLOUD_TF_DIR)/terraform.tfvars.example and edit it."; \
		exit 1; \
	fi

# One-time terraform init (local backend, no remote state).
cloud-init:
	terraform -chdir=$(CLOUD_TF_DIR) init

# Bring up the stack with .env.cloud sourced so Airflow containers start
# in cloud mode (PIPELINE_TARGET=cloud, GCP_* vars, keyfile mounted). Use
# this instead of `make up` when running the cloud pipeline end-to-end.
cloud-stack-up: _require-env-cloud
	$(CLOUD_ENV) && docker compose -f $(COMPOSE_FILE) up -d

# Preview every change. Mandatory before cloud-up per CLAUDE.md.
cloud-plan: _require-env-cloud _require-tfvars
	$(CLOUD_ENV) && terraform -chdir=$(CLOUD_TF_DIR) plan -var-file=terraform.tfvars

# Apply with an explicit typed confirmation — avoids accidental creation.
cloud-up: _require-env-cloud _require-tfvars
	@echo "About to create or update GCP resources via terraform apply."
	@read -p "Type 'apply' to confirm: " ans; [ "$$ans" = "apply" ] || (echo "Aborted."; exit 1)
	$(CLOUD_ENV) && terraform -chdir=$(CLOUD_TF_DIR) apply -var-file=terraform.tfvars

# Destroy everything managed by Terraform (including API activations so no
# residual per-month minimums remain). Typed confirmation required.
cloud-down: _require-env-cloud _require-tfvars
	@echo "About to DESTROY every GCP resource managed by Terraform."
	@read -p "Type 'destroy' to confirm: " ans; [ "$$ans" = "destroy" ] || (echo "Aborted."; exit 1)
	$(CLOUD_ENV) && terraform -chdir=$(CLOUD_TF_DIR) destroy -var-file=terraform.tfvars

# Upload Spark job sources to the Bronze bucket for Dataproc to pick up.
cloud-deploy-spark: _require-env-cloud
	$(CLOUD_ENV) && bash $(CLOUD_SCRIPTS_DIR)/deploy_spark_job.sh

# Trigger the ingestion DAG in cloud mode. Assumes `make cloud-stack-up`
# has been run so the airflow services are already in cloud mode.
cloud-run-ingestion: _require-env-cloud
	$(CLOUD_ENV) && docker compose -f $(COMPOSE_FILE) exec -T \
		-e PIPELINE_TARGET=cloud \
		airflow-webserver airflow dags trigger 01_velib_ingestion_pipeline

# dbt run/test against BigQuery. The container must already be built with
# the dual-adapter image (`make build`) and the keyfile must be mounted.
cloud-dbt-run: _require-env-cloud
	$(CLOUD_ENV) && docker exec -e PIPELINE_TARGET=cloud velib_dbt \
		dbt run --profiles-dir /usr/app/dbt --target bigquery_cloud

cloud-dbt-test: _require-env-cloud
	$(CLOUD_ENV) && docker exec -e PIPELINE_TARGET=cloud velib_dbt \
		dbt test --profiles-dir /usr/app/dbt --target bigquery_cloud

# Surface the last Dataproc Serverless batch description (status, URIs,
# stdout pointer). Bash script handles region + project resolution.
cloud-logs: _require-env-cloud
	$(CLOUD_ENV) && bash $(CLOUD_SCRIPTS_DIR)/run_dataproc_batch.sh --logs-only

# Backfill the local bronze data lake to GCS, then run Bronze->Silver
# (Dataproc) and Silver->Gold (dbt) only on the days missing in BigQuery.
# Default: dry-run. Pass extra flags via BACKFILL_ARGS, e.g.:
#   make cloud-backfill BACKFILL_ARGS="--from=2026-04-01 --skip-gold"
cloud-backfill: _require-env-cloud
	bash $(CLOUD_SCRIPTS_DIR)/backfill_local_to_cloud.sh $(BACKFILL_ARGS)

# Same as cloud-backfill but with --apply prepended. Typed confirmations
# inside the script still gate every cost-bearing step.
cloud-backfill-apply: _require-env-cloud
	bash $(CLOUD_SCRIPTS_DIR)/backfill_local_to_cloud.sh --apply $(BACKFILL_ARGS)

# Quick pointer to the billing dashboard. Live cost rollup via CLI needs
# billing API + org-level perms we don't assume here.
cloud-cost: _require-env-cloud
	@$(CLOUD_ENV) && echo "Billing console: https://console.cloud.google.com/billing (project $$GCP_PROJECT_ID)"
	@echo "Current month estimate:"
	@$(CLOUD_ENV) && gcloud billing projects describe $$GCP_PROJECT_ID --format='value(billingAccountName)' 2>/dev/null \
		| awk -F/ '{ print "  Linked to billing account: "$$2 }' || \
		echo "  (gcloud CLI not available or project not linked to a billing account)"

# ─────────────────────────────────────────────────────────────────────────────

# help
help:
	@echo "Available targets:"
	@echo "  build       : Build the images"
	@echo "  up          : Start the services"
	@echo "  up-logs     : Start with logs"
	@echo "  down        : Stop the services"
	@echo "  logs        : Show logs"
	@echo "  clean       : Clean everything"
	@echo "  first-launch: First launch (build + up)"
	@echo "  status      : Container status"
	@echo "  restart     : Restart a service (make restart SERVICE=<name>)"
	@echo "  shell       : Access a container (make shell SERVICE=<name>)"
	@echo "  fix-perms   : Give required permission to airflow and datalake folder in order to make docker-compose work (required in prod)"
	@echo "  give-perms  : Give permission to modify airflow and datalake folder  (usefull in devmode but useless in prod)"
	@echo "  dbt-deps    : Install dbt package dependencies"
	@echo "  dbt-run     : Run all dbt models (staging + gold)"
	@echo "  dbt-run-staging : Run only staging views"
	@echo "  dbt-run-gold    : Run only Gold models"
	@echo "  dbt-test    : Run dbt data-quality tests"
	@echo "  dbt-all     : Full dbt pipeline (deps → run → test)"
	@echo "  dbt-docs    : Generate and serve dbt docs on port 8082"
	@echo "  silver-schema : Create silver schema manually (if DB already existed)"
	@echo "  gold-schema : Create gold schema manually (if DB already existed)"
	@echo "  cleanup-parquet-build  : Build docker image for parquet cleanup script"
	@echo "  cleanup-parquet        : Dry-run scan of corrupted/duplicate parquet files"
	@echo "  cleanup-parquet-delete : Delete corrupted/duplicate parquet files"
	@echo "  ingest-build           : Build docker image for manual ingest_velib script"
	@echo "  ingest-manual          : Run ingest_velib once manually"
	@echo "  superset-db            : Create superset_meta database on postgres (idempotent)"
	@echo "  format-python          : Format Python code with Ruff"
	@echo "  lint-python            : Lint Python code with Ruff"
	@echo "  format-sql             : Auto-fix SQL style with SQLFluff"
	@echo "  lint-sql               : Lint SQL style with SQLFluff"
	@echo ""
	@echo "Cloud (GCP, needs .env.cloud + terraform.tfvars):"
	@echo "  cloud-init          : terraform init"
	@echo "  cloud-stack-up      : docker compose up with .env.cloud sourced (use instead of make up for cloud)"
	@echo "  cloud-plan          : terraform plan (always run before cloud-up)"
	@echo "  cloud-up            : terraform apply (typed confirmation)"
	@echo "  cloud-down          : terraform destroy (typed confirmation)"
	@echo "  cloud-deploy-spark  : upload spark_jobs/ to the Bronze bucket"
	@echo "  cloud-run-ingestion : trigger ingestion DAG with PIPELINE_TARGET=cloud"
	@echo "  cloud-dbt-run       : dbt run against BigQuery (bigquery_cloud target)"
	@echo "  cloud-dbt-test      : dbt test against BigQuery"
	@echo "  cloud-logs          : describe the latest Dataproc Serverless batch"
	@echo "  cloud-cost          : open the billing console for the project"
	@echo "  cloud-backfill      : dry-run backfill local bronze -> GCS -> BQ silver -> dbt gold"
	@echo "  cloud-backfill-apply: same as cloud-backfill but executes (typed confirmations)"
	@echo ""
	@echo "  help        : This help"