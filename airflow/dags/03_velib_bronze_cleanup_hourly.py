from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.pipeline_target import is_cloud

from airflow import DAG

SNAPSHOT_RE = re.compile(r"^snapshot_(\d{8})_(\d{6})\.parquet$")


def parse_snapshot_timestamp(path: Path) -> datetime | None:
    match = SNAPSHOT_RE.match(path.name)
    if not match:
        return None

    date_part, time_part = match.groups()
    try:
        return datetime.strptime(f"{date_part}_{time_part}", "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def is_parquet_valid(path: Path) -> tuple[bool, str | None]:
    """Strict parquet validation (metadata + data pages by row groups)."""
    try:
        parquet_file = pq.ParquetFile(path)
        num_row_groups = parquet_file.metadata.num_row_groups
        for row_group_idx in range(num_row_groups):
            parquet_file.read_row_group(row_group_idx)
        return True, None
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"


def cleanup_previous_hour_partition(**context):
    # Cloud mode uploads the Bronze snapshot straight after local validation
    # in the ingestion DAG, so every parquet landed in GCS is already proven
    # readable. Duplicates within the same minute are handled by the Silver
    # writer's dropDuplicates on (station_id, snapshot_timestamp), and long
    # term retention is enforced by the GCS bucket lifecycle rules. That
    # makes this local cleanup pass redundant when PIPELINE_TARGET=cloud.
    if is_cloud():
        print("⏭️  PIPELINE_TARGET=cloud, skipping local Bronze cleanup")
        context["ti"].xcom_push(key="skipped", value=True)
        return

    dt = context["data_interval_end"].in_timezone("Europe/Paris").subtract(hours=1)
    date = dt.format("YYYY-MM-DD")
    hour = dt.format("HH")

    partition_path = Path(f"/opt/airflow/data_lake/bronze/velib/ingestion_date={date}/hour={hour}")
    print(f"Scanning partition: {partition_path}")

    if not partition_path.exists() or not partition_path.is_dir():
        raise FileNotFoundError(f"Bronze partition not found: {partition_path}")

    parquet_files = sorted(partition_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in: {partition_path}")

    duplicate_files: set[Path] = set()
    corrupted_files: set[Path] = set()

    timestamped_files: list[tuple[datetime, Path]] = []
    for parquet_path in parquet_files:
        parsed = parse_snapshot_timestamp(parquet_path)
        if parsed is not None:
            timestamped_files.append((parsed, parquet_path))

    timestamped_files.sort(key=lambda item: (item[0], item[1].as_posix()))

    previous_minute = None
    previous_path = None
    for snapshot_dt, parquet_path in timestamped_files:
        current_minute = snapshot_dt.strftime("%Y%m%d_%H%M")
        if previous_minute == current_minute:
            duplicate_files.add(parquet_path)
            print(f"DUPLICATE: {parquet_path}")
            print(f"  Reason: same minute as previous snapshot ({previous_path})")
        previous_minute = current_minute
        previous_path = parquet_path

    for parquet_path in parquet_files:
        ok, error = is_parquet_valid(parquet_path)
        if ok:
            continue

        corrupted_files.add(parquet_path)
        print(f"CORRUPTED: {parquet_path}")
        print(f"  Reason: {error}")

    files_to_delete = sorted(corrupted_files | duplicate_files)
    deleted_count = 0
    for parquet_path in files_to_delete:
        try:
            os.remove(parquet_path)
            deleted_count += 1
            print(f"DELETE: {parquet_path}")
        except OSError as delete_error:
            print(f"DELETE FAILED: {parquet_path} ({delete_error})")
            raise

    remaining_files = sorted(partition_path.glob("*.parquet"))
    valid_remaining = []
    for parquet_path in remaining_files:
        ok, _ = is_parquet_valid(parquet_path)
        if ok:
            valid_remaining.append(parquet_path)

    print("Cleanup summary")
    print(f"  Duplicate files: {len(duplicate_files)}")
    print(f"  Corrupted files: {len(corrupted_files)}")
    print(f"  Deleted files: {deleted_count}")
    print(f"  Remaining files: {len(remaining_files)}")
    print(f"  Remaining valid files: {len(valid_remaining)}")

    if not valid_remaining:
        raise ValueError(f"No valid parquet files remain in {partition_path} after cleanup")

    context["ti"].xcom_push(key="partition_path", value=str(partition_path))
    context["ti"].xcom_push(key="remaining_valid_files", value=len(valid_remaining))


DEFAULT_ARGS = {
    "owner": "velib_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="03_velib_bronze_cleanup_hourly",
    default_args=DEFAULT_ARGS,
    description="Cleanup hourly bronze parquet partition before silver transformation",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["velib", "bronze", "cleanup", "hourly"],
) as dag:
    cleanup_task = PythonOperator(
        task_id="cleanup_previous_hour_partition",
        python_callable=cleanup_previous_hour_partition,
        execution_timeout=timedelta(minutes=20),
    )

    trigger_silver_task = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",
        trigger_dag_id="04_velib_silver_transformation_hourly",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    cleanup_task >> trigger_silver_task
