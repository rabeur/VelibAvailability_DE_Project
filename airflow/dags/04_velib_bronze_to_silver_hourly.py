from datetime import datetime, timedelta
import glob
import os
from urllib.parse import urlparse

from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2


default_args = {
    "owner": "velib_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _get_previous_hour_partition(context) -> tuple[str, str]:
    dt = context["data_interval_end"].in_timezone("Europe/Paris").subtract(hours=1)
    return dt.format("YYYY-MM-DD"), dt.format("HH")


def _get_dw_connection() -> psycopg2.extensions.connection:
    """Build a psycopg2 connection from the Airflow SQLAlchemy connection env var."""
    conn_str = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    normalized_conn_str = conn_str.replace("postgresql+psycopg2://", "postgresql://").replace(
        "postgresql+psycopg://", "postgresql://"
    )
    parsed = urlparse(normalized_conn_str)

    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        dbname=parsed.path.lstrip("/"),
    )


def check_bronze_data(**context) -> bool:
    """Ensure Bronze data exists for the previous hour."""
    date, hour = _get_previous_hour_partition(context)
    bronze_path = f"/opt/airflow/data_lake/bronze/velib/ingestion_date={date}/hour={hour}"

    print(f"Checking Bronze data for {date} at {hour}:00")

    if not os.path.isdir(bronze_path):
        raise FileNotFoundError(f"Bronze partition not found: {bronze_path}")

    parquet_files = glob.glob(f"{bronze_path}/*.parquet")
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in partition: {bronze_path}")

    print(f"Found {len(parquet_files)} parquet file(s) for {date} {hour}:00")
    return True


def run_spark_job(**context) -> bool:
    """Run the Spark bronze_to_silver job inside the velib_spark container."""
    import docker as docker_sdk

    date, hour = _get_previous_hour_partition(context)

    print(f"Starting Spark transformation for {date} at {hour}:00")

    client = docker_sdk.from_env()
    container = client.containers.get("velib_spark")

    # Ensure PostgreSQL JDBC driver is available for Spark writes.
    _, dl_output = container.exec_run(
        "bash -c 'if [ ! -f /opt/spark/jars/postgresql-42.7.1.jar ]; then "
        "wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar "
        "-O /opt/spark/jars/postgresql-42.7.1.jar && echo Driver downloaded; fi'"
    )
    if dl_output:
        print(dl_output.decode("utf-8", errors="replace"))

    command = (
        "/opt/spark/bin/spark-submit "
        "--jars /opt/spark/jars/postgresql-42.7.1.jar "
        "--master local[*] "
        "--driver-memory 2g "
        "--executor-memory 2g "
        "--conf spark.sql.shuffle.partitions=10 "
        "/opt/spark_jobs/bronze_to_silver.py "
        f"/opt/data_lake/bronze/velib {date} {hour}"
    )
    print(f"Spark command: {command}")

    exit_code, output = container.exec_run(command, stream=False)
    if output:
        print(output.decode("utf-8", errors="replace"))

    if exit_code != 0:
        raise RuntimeError(f"Spark job failed with return code {exit_code}")

    print(f"Spark transformation for {date} at {hour}:00 succeeded")
    return True


def validate_silver_data(**context) -> bool:
    """Validate Silver rows loaded for the previous hour."""
    date, hour = _get_previous_hour_partition(context)
    hour_timestamp = f"{date} {hour.zfill(2)}:00:00"

    print(f"Validating Silver data for {date} at {hour}:00")

    conn = _get_dw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT station_id) AS unique_stations,
                    MIN(snapshot_timestamp) AS earliest,
                    MAX(snapshot_timestamp) AS latest
                FROM silver.station_availability
                WHERE snapshot_timestamp >= %s::timestamp
                  AND snapshot_timestamp < %s::timestamp + interval '1 hour'
                """,
                (hour_timestamp, hour_timestamp),
            )
            total_rows, unique_stations, earliest, latest = cur.fetchone()
    finally:
        conn.close()

    print(f"total_rows={total_rows}, unique_stations={unique_stations}")
    print(f"earliest={earliest}, latest={latest}")

    if total_rows == 0:
        raise ValueError(f"No Silver data found for {date} at {hour}:00")

    print(f"Silver validation succeeded with {total_rows} rows")
    return True


def show_summary() -> None:
    """Display a compact summary of Silver tables."""
    print("Building Silver data summary")

    conn = _get_dw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.stations;")
            total_stations = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM silver.station_availability;")
            total_snapshots = cur.fetchone()[0]

            cur.execute(
                """
                SELECT
                    date_trunc('hour', snapshot_timestamp) AS snapshot_hour,
                    COUNT(*) AS rows_added,
                    COUNT(DISTINCT station_id) AS stations_count
                FROM silver.station_availability
                GROUP BY date_trunc('hour', snapshot_timestamp)
                ORDER BY snapshot_hour DESC
                LIMIT 1
                """
            )
            last_snapshot = cur.fetchone()
    finally:
        conn.close()

    print(f"Total stations: {total_stations}")
    print(f"Total snapshots: {total_snapshots}")

    if last_snapshot:
        snapshot_hour, rows_added, stations_count = last_snapshot
        print("Latest snapshot loaded:")
        print(f"  date={snapshot_hour.strftime('%Y-%m-%d')}")
        print(f"  time={snapshot_hour.strftime('%H:%M:%S')}")
        print(f"  rows_added={rows_added}")
        print(f"  stations_count={stations_count}")
    else:
        print("No snapshot available in silver.station_availability")


with DAG(
    dag_id="velib_silver_transformation_hourly",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["velib", "bronze", "silver", "spark", "hourly"],
) as dag:
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=check_bronze_data,
    )

    run_spark_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )

    validate_task = PythonOperator(
        task_id="validate_silver_data",
        python_callable=validate_silver_data,
    )

    summary_task = PythonOperator(
        task_id="show_summary",
        python_callable=show_summary,
    )

    load_task >> run_spark_task >> validate_task >> summary_task
