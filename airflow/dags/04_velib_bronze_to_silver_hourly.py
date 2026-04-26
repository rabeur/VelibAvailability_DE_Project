import glob
import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import psycopg2
from airflow.operators.python import PythonOperator
from utils.pipeline_target import get_cloud_config, is_cloud

from airflow import DAG

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
    if is_cloud():
        return _check_bronze_data_gcs(date, hour)
    return _check_bronze_data_local(date, hour)


def _check_bronze_data_local(date: str, hour: str) -> bool:
    bronze_path = f"/opt/airflow/data_lake/bronze/velib/ingestion_date={date}/hour={hour}"

    print(f"Checking Bronze data for {date} at {hour}:00")

    if not os.path.isdir(bronze_path):
        raise FileNotFoundError(f"Bronze partition not found: {bronze_path}")

    parquet_files = glob.glob(f"{bronze_path}/*.parquet")
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in partition: {bronze_path}")

    print(f"Found {len(parquet_files)} parquet file(s) for {date} {hour}:00")
    return True


def _check_bronze_data_gcs(date: str, hour: str) -> bool:
    # Lazy import so a pure local install without apache-airflow-providers-google
    # keeps DAG parsing green.
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    cloud_config = get_cloud_config()
    prefix = f"bronze/velib/ingestion_date={date}/hour={hour}/"

    print(f"Checking GCS Bronze prefix gs://{cloud_config.bronze_bucket}/{prefix}")

    hook = GCSHook()
    objects = hook.list(bucket_name=cloud_config.bronze_bucket, prefix=prefix)
    parquet_objects = [obj for obj in objects if obj.endswith(".parquet")]

    if not parquet_objects:
        raise FileNotFoundError(
            f"No parquet objects found at gs://{cloud_config.bronze_bucket}/{prefix}"
        )

    print(
        f"Found {len(parquet_objects)} parquet object(s) at gs://{cloud_config.bronze_bucket}/{prefix}"
    )
    return True


def run_spark_job(**context) -> bool:
    """Run the Spark bronze_to_silver job (local Docker exec or Dataproc Serverless batch)."""
    date, hour = _get_previous_hour_partition(context)
    if is_cloud():
        return _run_spark_cloud(context, date, hour)
    return _run_spark_local(date, hour)


def _run_spark_local(date: str, hour: str) -> bool:
    import docker as docker_sdk

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
        # Python workers need these modules on sys.path to unpickle UDFs that
        # import them (driver sees them via the main file's directory, workers
        # don't — hence --py-files for local[*] mode).
        "--py-files /opt/spark_jobs/paris_arrondissement_utils.py,/opt/spark_jobs/silver_writers.py,/opt/spark_jobs/data/paris_arrondissements.geojson "
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


def _run_spark_cloud(context: dict, date: str, hour: str) -> bool:
    """Submit a Dataproc Serverless PySpark batch and wait for completion.

    The Silver writer already reads PIPELINE_TARGET at runtime on the driver
    side and routes to BigQuery, so the only cloud-specific work here is the
    batch submission itself. Batch IDs embed the hour and a short random
    suffix so that manual re-runs of the same partition never collide with a
    previous submission kept in Dataproc history.
    """
    from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

    cloud_config = get_cloud_config()
    short_id = uuid.uuid4().hex[:6]
    batch_id = f"velib-silver-{date.replace('-', '')}-{hour}-{short_id}"

    print(
        f"Submitting Dataproc Serverless batch {batch_id} in region {cloud_config.region} "
        f"for partition {date}/{hour}"
    )

    # Spark driver inherits PIPELINE_TARGET=cloud plus the GCP env vars the
    # silver writer needs; values are forwarded via runtime properties.
    # Dataproc Serverless uses spark.driverEnv.* for the driver and
    # spark.executorEnv.* for executors — spark.yarn.appMasterEnv.* is
    # rejected (that prefix only applies to YARN-backed Dataproc clusters).
    spark_env_properties = {
        "spark.executorEnv.PIPELINE_TARGET": "cloud",
        "spark.executorEnv.GCP_PROJECT_ID": cloud_config.project_id,
        "spark.executorEnv.GCP_BRONZE_BUCKET": cloud_config.bronze_bucket,
        "spark.executorEnv.GCP_BIGQUERY_SILVER_DATASET": cloud_config.silver_dataset,
        "spark.executorEnv.GCP_BQ_TEMP_BUCKET": cloud_config.temp_bucket,
        "spark.driverEnv.PIPELINE_TARGET": "cloud",
        "spark.driverEnv.GCP_PROJECT_ID": cloud_config.project_id,
        "spark.driverEnv.GCP_BRONZE_BUCKET": cloud_config.bronze_bucket,
        "spark.driverEnv.GCP_BIGQUERY_SILVER_DATASET": cloud_config.silver_dataset,
        "spark.driverEnv.GCP_BQ_TEMP_BUCKET": cloud_config.temp_bucket,
        "spark.executor.instances": "2",
        "spark.executor.memory": "4g",
    }

    # Modules imported by bronze_to_silver must be distributed explicitly
    # (Dataproc Serverless only copies main_python_file_uri by default).
    # Same rationale as the local --py-files fix.
    support_uri = f"gs://{cloud_config.bronze_bucket}/spark_jobs"
    batch = {
        "pyspark_batch": {
            "main_python_file_uri": cloud_config.spark_job_uri,
            "python_file_uris": [
                f"{support_uri}/paris_arrondissement_utils.py",
                f"{support_uri}/silver_writers.py",
            ],
            "file_uris": [
                f"{support_uri}/data/paris_arrondissements.geojson",
            ],
            # Pass GCP config as named CLI overrides because Dataproc
            # Serverless does not propagate spark.driverEnv.* / executorEnv.*
            # into the driver process env reliably. bronze_to_silver.py
            # injects these back into os.environ at the start of main().
            "args": [
                "cloud",
                date,
                hour,
                f"--bronze-bucket={cloud_config.bronze_bucket}",
                f"--gcp-project={cloud_config.project_id}",
                f"--silver-dataset={cloud_config.silver_dataset}",
                f"--temp-bucket={cloud_config.temp_bucket}",
            ],
        },
        "runtime_config": {"properties": spark_env_properties},
    }

    # Dataproc Serverless v1 EnvironmentConfig only accepts execution_config
    # and peripherals_config — there is no top-level environment_variables
    # field. Driver/executor env vars therefore travel exclusively through
    # spark.driverEnv.* / spark.executorEnv.* in runtime_config.properties.
    if cloud_config.service_account:
        batch["environment_config"] = {
            "execution_config": {"service_account": cloud_config.service_account},
        }

    # Instantiate the native operator without registering it in the DAG so we
    # get polling, error surfacing, and Airflow connections handling for free
    # while keeping the graph identical between local and cloud modes.
    operator = DataprocCreateBatchOperator(
        task_id="dataproc_submit_inline",
        project_id=cloud_config.project_id,
        region=cloud_config.region,
        batch=batch,
        batch_id=batch_id,
    )
    operator.execute(context=context)

    print(f"Dataproc batch {batch_id} completed successfully")
    return True


def validate_silver_data(**context) -> bool:
    """Validate Silver rows loaded for the previous hour."""
    date, hour = _get_previous_hour_partition(context)
    hour_timestamp = f"{date} {hour.zfill(2)}:00:00"

    print(f"Validating Silver data for {date} at {hour}:00")

    if is_cloud():
        return _validate_silver_bigquery(date, hour, hour_timestamp)
    return _validate_silver_postgres(date, hour, hour_timestamp)


def _validate_silver_postgres(date: str, hour: str, hour_timestamp: str) -> bool:
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


def _validate_silver_bigquery(date: str, hour: str, hour_timestamp: str) -> bool:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    cloud_config = get_cloud_config()
    table = f"`{cloud_config.project_id}.{cloud_config.silver_dataset}.station_availability`"
    # Hour-bounded window prunes to a single daily partition since the table
    # is partitioned on snapshot_timestamp — no cost surprises here.
    sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT station_id) AS unique_stations,
            MIN(snapshot_timestamp) AS earliest,
            MAX(snapshot_timestamp) AS latest
        FROM {table}
        WHERE snapshot_timestamp >= TIMESTAMP('{hour_timestamp}')
          AND snapshot_timestamp < TIMESTAMP_ADD(TIMESTAMP('{hour_timestamp}'), INTERVAL 1 HOUR)
    """

    # location is mandatory on get_records; pass the dataset region so the
    # hook does not raise "Need to specify 'location'" before issuing the
    # query.
    hook = BigQueryHook(use_legacy_sql=False, location=cloud_config.region)
    rows = hook.get_records(sql)
    total_rows, unique_stations, earliest, latest = rows[0]

    print(f"total_rows={total_rows}, unique_stations={unique_stations}")
    print(f"earliest={earliest}, latest={latest}")

    if total_rows == 0:
        raise ValueError(f"No Silver data found in BigQuery for {date} at {hour}:00")

    print(f"Silver validation (BigQuery) succeeded with {total_rows} rows")
    return True


def show_summary(**context) -> None:
    """Display a compact summary of Silver tables."""
    print("Building Silver data summary")

    if is_cloud():
        _show_summary_bigquery()
    else:
        _show_summary_postgres()


def _show_summary_postgres() -> None:
    conn = _get_dw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.stations;")
            total_stations = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM silver.station_availability;")
            total_snapshots = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    date_trunc('hour', snapshot_timestamp) AS snapshot_hour,
                    COUNT(*) AS rows_added,
                    COUNT(DISTINCT station_id) AS stations_count
                FROM silver.station_availability
                GROUP BY date_trunc('hour', snapshot_timestamp)
                ORDER BY snapshot_hour DESC
                LIMIT 1
                """)
            last_snapshot = cur.fetchone()
    finally:
        conn.close()

    _log_summary(total_stations, total_snapshots, last_snapshot)


def _show_summary_bigquery() -> None:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    cloud_config = get_cloud_config()
    stations_table = f"`{cloud_config.project_id}.{cloud_config.silver_dataset}.stations`"
    availability_table = (
        f"`{cloud_config.project_id}.{cloud_config.silver_dataset}.station_availability`"
    )

    hook = BigQueryHook(use_legacy_sql=False, location=cloud_config.region)

    total_stations = hook.get_records(f"SELECT COUNT(*) FROM {stations_table}")[0][0]
    total_snapshots = hook.get_records(f"SELECT COUNT(*) FROM {availability_table}")[0][0]

    # Scope the latest-hour scan to the last 24h of partitions to keep the
    # summary query in the "few MB scanned" range for BigQuery billing.
    latest = hook.get_records(f"""
        SELECT
            TIMESTAMP_TRUNC(snapshot_timestamp, HOUR) AS snapshot_hour,
            COUNT(*) AS rows_added,
            COUNT(DISTINCT station_id) AS stations_count
        FROM {availability_table}
        WHERE snapshot_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY snapshot_hour
        ORDER BY snapshot_hour DESC
        LIMIT 1
        """)
    last_snapshot = latest[0] if latest else None

    # BigQueryHook.get_records returns TIMESTAMP values as UNIX epoch floats,
    # whereas psycopg2 yields datetime objects. Normalise here so _log_summary
    # can call .strftime() identically for both targets.
    if last_snapshot is not None:
        snapshot_hour, rows_added, stations_count = last_snapshot
        if isinstance(snapshot_hour, (int, float)):
            snapshot_hour = datetime.fromtimestamp(snapshot_hour, tz=timezone.utc)
        last_snapshot = (snapshot_hour, rows_added, stations_count)

    _log_summary(total_stations, total_snapshots, last_snapshot)


def _log_summary(total_stations: int, total_snapshots: int, last_snapshot) -> None:
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
    dag_id="04_velib_silver_transformation_hourly",
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
