from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime, timedelta
import subprocess
import psycopg2
from urllib.parse import urlparse

default_args = {
    "owner": "velib_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_command(command: str, description: str) -> bool:
    """
    Run a shell command and print the result

    Returns:
        True on success, otherwise False
    """
    print(f"\n{'='*70}")
    print(f"🔧 {description}")
    print(f"{'='*70}")
    print(f"Commande: {command}\n")

    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )

        if result.stdout:
            print(result.stdout)

        print(f"✅ {description} - Success")
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - Failed")
        print(f"Return code: {e.returncode}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False


def check_bronze_data(**context) -> bool:
    """Check whether Bronze data exists for the previous hour"""
    dt = context["data_interval_end"].in_timezone("Europe/Paris").subtract(hours=1)  # Check the previous hour to ensure the data is available
    date = dt.format("YYYY-MM-DD")
    hour = dt.format("HH")

    print(f"\n🔍 Checking Bronze data for {date} at {hour}:00...")

    base_path = "/opt/airflow/data_lake/bronze/velib"


    bronze_path = f"{base_path}/ingestion_date={date}/hour={hour}"
    if os.path.isdir(bronze_path):
        import glob
        files = glob.glob(f"{bronze_path}/*.parquet")
        file_count = len(files)
        if file_count > 0:
            print(f"  ✅ {file_count} Parquet files found for {hour}:00")
            return True

    print(f"  ❌ No Bronze data for {date} at {hour}:00")
    print(f"     Checked path: {bronze_path}")
    return False

def run_spark_job(**context) -> bool:
    """Run the Spark job through the Docker Python SDK (mounted /var/run/docker.sock)"""
    import docker as docker_sdk

    dt = context["data_interval_end"].in_timezone("Europe/Paris").subtract(hours=1)
    date = dt.format("YYYY-MM-DD")
    hour = dt.format("HH")

    print(f"\n{'='*70}")
    print(f"🔧 Spark transformation for {date} at {hour}:00")
    print(f"{'='*70}")

    client = docker_sdk.from_env()
    container = client.containers.get("velib_spark")

    # Download the PostgreSQL driver if needed
    _, dl_output = container.exec_run(
        "bash -c 'if [ ! -f /opt/spark/jars/postgresql-42.7.1.jar ]; then "
        "wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar "
        "-O /opt/spark/jars/postgresql-42.7.1.jar && echo Driver downloaded; fi'"
    )
    if dl_output:
        print(dl_output.decode("utf-8", errors="replace"))

    cmd = (
        "/opt/spark/bin/spark-submit "
        "--jars /opt/spark/jars/postgresql-42.7.1.jar "
        "--master local[*] "
        "--driver-memory 2g "
        "--executor-memory 2g "
        "--conf spark.sql.shuffle.partitions=10 "
        f"/opt/spark_jobs/bronze_to_silver.py "
        f"/opt/data_lake/bronze/velib "
        f"{date} {hour}"
    )
    print(f"Command: {cmd}\n")

    exit_code, output = container.exec_run(cmd, stream=False)
    if output:
        print(output.decode("utf-8", errors="replace"))

    if exit_code != 0:
        raise Exception(f"The Spark job failed with return code {exit_code}")

    print(f"✅ Spark transformation for {date} at {hour}:00 - Success")
    return True

def validate_silver_data(**context) -> bool:
    """Validate that data was loaded into Silver using a direct psycopg2 connection"""
    import psycopg2
    from urllib.parse import urlparse

    dt = context["data_interval_end"].in_timezone("Europe/Paris").subtract(hours=1)
    date = dt.format("YYYY-MM-DD")
    hour = dt.format("HH")

    print(f"\n📊 Validating Silver data for {date} at {hour}:00")

    # Parse the Airflow connection string: postgresql+psycopg2://user:pass@host:port/db
    conn_str = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    parsed = urlparse(conn_str.replace("postgresql+psycopg2://", "postgresql://"))

    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        dbname=parsed.path.lstrip("/"),
    )

    hour_timestamp = f"{date} {hour.zfill(2)}:00:00"
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
            row = cur.fetchone()
            total_rows, unique_stations, earliest, latest = row
            print(f"  total_rows={total_rows}, unique_stations={unique_stations}")
            print(f"  earliest={earliest}, latest={latest}")
    finally:
        conn.close()

    if total_rows == 0:
        print("  ❌ No Silver data found for this hour")
        return False

    print(f"  ✅ {total_rows} rows validated for {unique_stations} stations")
    return True

def show_summary():
    """Display a summary of Silver data"""


    print("\n📈 Silver data summary...")

    # Parse the Airflow connection string: postgresql+psycopg2://user:pass@host:port/db
    conn_str = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    parsed = urlparse(conn_str.replace("postgresql+psycopg2://", "postgresql://"))

    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        dbname=parsed.path.lstrip("/"),
    )

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

            print(f"  ✅ Total number of stations: {total_stations}")
            print(f"  ✅ Total number of snapshots: {total_snapshots}")

            if last_snapshot:
                snapshot_hour, rows_added, stations_count = last_snapshot
                print("  ✅ Latest snapshot added:")
                print(f"     - Date: {snapshot_hour.strftime('%Y-%m-%d')}")
                print(f"     - Time: {snapshot_hour.strftime('%H:%M:%S')}")
                print(f"     - Rows added: {rows_added}")
                print(f"     - Stations: {stations_count}")
            else:
                print("  ⚠️ No snapshot available in silver.station_availability")
    finally:
        conn.close()



with DAG(
    dag_id="velib_silver_transformation_hourly",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["velib", "bronze", "silver", "spark", "hourly"],
) as dag:

    # Task 1: Check Bronze data for the previous hour
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=check_bronze_data,
    )

    # Task 2: Run the Spark job to transform data from Bronze to Silver
    run_spark_job = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
    )

    # Task 3: Validate Silver data
    validate_task = PythonOperator(
        task_id='validate_silver_data',
        python_callable=validate_silver_data,
    )

    # Task 4: Show a summary of Silver data
    summary_task = PythonOperator(
        task_id='show_summary',
        python_callable=show_summary,
    )

    load_task >> run_spark_job >> validate_task >> summary_task