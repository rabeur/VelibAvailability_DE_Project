"""
Airflow DAG: velib_dbt_gold_transformation
==========================================
Runs the dbt Gold layer pipeline daily at 03:00 (after the Silver hourly pipeline).
Uses the same Docker SDK pattern as the Bronze→Silver DAG:
  - exec commands into the running `velib_dbt` container.

Task flow:
  dbt_deps  →  dbt_run_staging  →  dbt_run_gold  →  dbt_test
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "velib_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DBT_WORKDIR = "/usr/app/dbt"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _exec_dbt(dbt_command: str) -> None:
    """
    Run a dbt CLI command inside the running `velib_dbt` container
    via the Docker Python SDK (mounted /var/run/docker.sock).

    Args:
        dbt_command: dbt sub-command and flags, e.g. "run --select gold"
    """
    import docker as docker_sdk

    client = docker_sdk.from_env()
    container = client.containers.get("velib_dbt")

    full_cmd = f"dbt {dbt_command} --profiles-dir {DBT_WORKDIR}"
    print(f"\n{'=' * 70}")
    print(f"🔧 Running: {full_cmd}")
    print(f"{'=' * 70}\n")

    exit_code, output = container.exec_run(
        full_cmd,
        workdir=DBT_WORKDIR,
        stream=False,
    )

    if output:
        print(output.decode("utf-8", errors="replace"))

    if exit_code != 0:
        raise RuntimeError(f"dbt command failed (exit_code={exit_code}): {full_cmd}")

    print(f"✅ dbt {dbt_command} — Success")


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def task_dbt_deps(**context):
    """Install dbt package dependencies (dbt-utils etc.)."""
    _exec_dbt("deps")


def task_dbt_run_staging(**context):
    """Run staging views on top of the Silver layer."""
    _exec_dbt("run --select staging")


def task_dbt_run_gold(**context):
    """
    Run all Gold layer models:
      - dim_stations (full refresh)
      - fact_hourly_availability (incremental)
      - fact_daily_station_stats (incremental)
      - mart_station_performance (full refresh)
      - mart_peak_hours_by_district (full refresh)
    """
    _exec_dbt("run --select gold")


def task_dbt_test(**context):
    """Run dbt data-quality tests on all Gold models."""
    _exec_dbt("test --select gold staging")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="velib_dbt_gold_transformation",
    default_args=default_args,
    description="Build the Gold analytical layer from Silver data using dbt",
    schedule_interval="0 3 * * *",  # Daily at 03:00 — after Silver pipeline (hourly)
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "gold", "transformation"],
) as dag:
    dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=task_dbt_deps,
        doc_md="Install dbt package dependencies declared in packages.yml",
    )

    dbt_run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=task_dbt_run_staging,
        doc_md="Materialise staging views on top of silver.stations and silver.station_availability",
    )

    dbt_run_gold = PythonOperator(
        task_id="dbt_run_gold",
        python_callable=task_dbt_run_gold,
        doc_md="Build Gold dimensions, incremental fact tables, and analytics marts",
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=task_dbt_test,
        doc_md="Run dbt schema + data tests on Gold and staging models",
    )

    dbt_deps >> dbt_run_staging >> dbt_run_gold >> dbt_test
