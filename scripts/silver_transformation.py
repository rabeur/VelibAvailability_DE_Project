#!/usr/bin/env python3
"""
Manual test script for the Bronze -> Silver transformation
Allows testing the pipeline without going through Airflow
"""

import sys
import os
from datetime import datetime, timedelta
import subprocess
import argparse


def run_command(command: str, description: str) -> bool:
    """
    Run a shell command and print the result

    Returns:
        True on success, otherwise False
    """
    print(f"\n{'=' * 70}")
    print(f"🔧 {description}")
    print(f"{'=' * 70}")
    print(f"Commande: {command}\n")

    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)

        if result.stdout:
            print(result.stdout)

        print(f"✅ {description} - Succès")
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - Échec")
        print(f"Code de retour: {e.returncode}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False


def check_prerequisites():
    """Check that all prerequisites are available"""
    print("\n📋 Checking prerequisites...")

    checks = {
        "Docker": "docker --version",
        "Docker Compose": "docker-compose --version",
        "Conteneur Postgres": "docker ps | grep velib_postgres",
        "Conteneur Spark": "docker ps | grep velib_spark",
        "Conteneur Airflow": "docker ps | grep velib_airflow",
    }

    all_ok = True
    for name, command in checks.items():
        result = subprocess.run(command, shell=True, capture_output=True)
        if result.returncode == 0:
            print(f"  ✅ {name}")
        else:
            print(f"  ❌ {name} - Not available")
            all_ok = False

    return all_ok


def check_bronze_data(date: str, hour: str | None = None) -> bool:
    """Check whether Bronze data exists for the hour or the full day"""
    if hour is not None:
        print(f"\n🔍 Checking Bronze data for {date} at {hour}:00...")
    else:
        print(f"\n🔍 Checking Bronze data for {date} (full day)...")

    date_part = date

    if hour is not None:
        command = f"""
        docker exec velib_airflow_scheduler bash -c "
            if [ -d '/opt/airflow/data_lake/bronze/velib/ingestion_date={date_part}/hour={hour}' ]; then
                echo 'FOUND'
            else
                echo 'NOT_FOUND'
            fi
        "
        """
    else:
        command = f"""
        docker exec velib_airflow_scheduler bash -c "
            if [ -d '/opt/airflow/data_lake/bronze/velib/ingestion_date={date_part}' ]; then
                echo 'FOUND'
            else
                echo 'NOT_FOUND'
            fi
        "
        """

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if "FOUND" in result.stdout:
        if hour is not None:
            # Count files for this specific hour
            count_cmd = f"""
            docker exec velib_airflow_scheduler bash -c "
                find /opt/airflow/data_lake/bronze/velib/ingestion_date={date_part}/hour={hour} -name '*.parquet' | wc -l
            "
            """
            count_result = subprocess.run(count_cmd, shell=True, capture_output=True, text=True)
            file_count = int(count_result.stdout.strip())
            print(f"  ✅ {file_count} Parquet files found for {hour}:00")
        else:
            # Count files for the full date
            count_cmd = f"""
            docker exec velib_airflow_scheduler bash -c "
                find /opt/airflow/data_lake/bronze/velib/ingestion_date={date_part} -name '*.parquet' | wc -l
            "
            """
            count_result = subprocess.run(count_cmd, shell=True, capture_output=True, text=True)
            file_count = int(count_result.stdout.strip())
            print(f"  ✅ {file_count} Parquet files found for date {date_part}")
        return file_count > 0
    else:
        if hour is not None:
            print(f"  ❌ No Bronze data for {date_part} at {hour}:00")
            print(
                f"     Checked path: /opt/airflow/data_lake/bronze/velib/ingestion_date={date_part}/hour={hour}"
            )
        else:
            print(f"  ❌ No Bronze data for {date_part}")
            print(
                f"     Checked path: /opt/airflow/data_lake/bronze/velib/ingestion_date={date_part}"
            )
        return False


def run_spark_job(date: str, hour: str | None = None) -> bool:
    """Run the Spark transformation job"""
    # Download the PostgreSQL driver if needed
    download_cmd = """
    docker exec velib_spark bash -c "
        if [ ! -f /opt/spark/jars/postgresql-42.7.1.jar ]; then
            wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O /opt/spark/jars/postgresql-42.7.1.jar
        fi
    "
    """
    subprocess.run(download_cmd, shell=True, check=True)

    if hour is not None:
        command = f"""
        docker exec velib_spark /opt/spark/bin/spark-submit \
            --jars /opt/spark/jars/postgresql-42.7.1.jar \
            --master local[*] \
            --driver-memory 2g \
            --executor-memory 2g \
            --conf spark.sql.shuffle.partitions=10 \
            /opt/spark_jobs/bronze_to_silver.py \
            /opt/data_lake/bronze/velib \
            {date} \
            {hour}
        """
        return run_command(command, f"Spark transformation for {date} at {hour}:00")

    # If no hour is provided, process all hours of the day through the BronzeToSilver pipeline
    command = f"""
    docker exec velib_spark /opt/spark/bin/spark-submit \
        --jars /opt/spark/jars/postgresql-42.7.1.jar \
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf spark.sql.shuffle.partitions=10 \
        /opt/spark_jobs/bronze_to_silver.py \
        /opt/data_lake/bronze/velib \
        {date}
    """
    return run_command(command, f"Spark transformation for full date {date}")


def validate_silver_data(date: str, hour: str | None = None):
    """Validate that data has been loaded into Silver"""
    if hour is not None:
        print(f"\n📊 Validating Silver data for {date} at {hour}:00")
        hour_timestamp = f"{date} {hour.zfill(2)}:00:00"
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT station_id) as unique_stations,
            MIN(snapshot_timestamp) as earliest,
            MAX(snapshot_timestamp) as latest
        FROM silver.station_availability
        WHERE snapshot_timestamp >= '{hour_timestamp}'::timestamp
        AND snapshot_timestamp < ('{hour_timestamp}'::timestamp + interval '1 hour');
        """
    else:
        print(f"\n📊 Validating Silver data for date {date} (full day)")
        day_start = f"{date} 00:00:00"
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT station_id) as unique_stations,
            MIN(snapshot_timestamp) as earliest,
            MAX(snapshot_timestamp) as latest
        FROM silver.station_availability
        WHERE snapshot_timestamp >= '{day_start}'::timestamp
        AND snapshot_timestamp < ('{day_start}'::timestamp + interval '1 day');
        """

    command = f"""docker exec -i velib_postgres psql -U velib -d velib_dw -c "{query}" """

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    print(result.stdout)

    if "0 rows" in result.stdout or "ERROR" in result.stderr:
        print("  ❌ No data found or query error")
        return False
    else:
        print("  ✅ Data validated")
        return True


def show_summary():
    """Print a summary of Silver data"""
    print("\n📈 Silver data summary...")

    queries = [
        ("Total number of stations", "SELECT COUNT(*) as total FROM silver.stations;"),
        ("Total number of snapshots", "SELECT COUNT(*) as total FROM silver.station_availability;"),
        (
            "Latest hourly snapshots",
            """SELECT
                date_trunc('hour', snapshot_timestamp) as hour,
                COUNT(*) as snapshots,
                COUNT(DISTINCT station_id) as stations
            FROM silver.station_availability
            GROUP BY date_trunc('hour', snapshot_timestamp)
            ORDER BY hour DESC
            LIMIT 5;""",
        ),
    ]

    for title, query in queries:
        print(f"\n{title}:")
        command = f"""docker exec -i velib_postgres psql -U velib -d velib_dw -c "{query}" """
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(result.stdout)


def main():
    parser = argparse.ArgumentParser(
        description="Manual test for the Bronze -> Silver transformation"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=(datetime.now()).strftime("%Y-%m-%d"),
        help="Day to process (format YYYY-MM-DD, default: current day)",
    )
    parser.add_argument(
        "--hour",
        type=str,
        default=None,
        help="Hour to process (format HH, optional: if omitted, processes the full day)",
    )
    parser.add_argument(
        "--skip-validation", action="store_true", help="Skip prerequisite validation"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Display only the summary (without running the job)",
    )

    args = parser.parse_args()

    print("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║   MANUAL TEST - BRONZE -> SILVER TRANSFORMATION                    ║
    ║   Velib Data Engineering Project                                   ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)

    print(f"📅 Target day: {args.date}")
    if args.hour:
        print(f"📅 Target hour: {args.hour}:00")

    # Summary-only mode
    if args.summary_only:
        show_summary()
        return

    # Check prerequisites
    if not args.skip_validation:
        if not check_prerequisites():
            print("\n❌ Missing prerequisites. Start them first with 'docker-compose up -d'")
            sys.exit(1)

    # Check Bronze data
    if not check_bronze_data(args.date, args.hour):
        if args.hour:
            print(f"\n❌ Missing Bronze data for {args.date} at {args.hour}:00")
        else:
            print(f"\n❌ Missing Bronze data for date {args.date}")
        print("   Start the Bronze ingestion DAG first")
        sys.exit(1)

    # Run the Spark job
    if not run_spark_job(args.date, args.hour):
        if args.hour:
            print("\n❌ Spark job failed for the requested hour")
        else:
            print("\n❌ Spark job failed for the full date")
        sys.exit(1)

    # Validate results
    if not validate_silver_data(args.date, args.hour):
        if args.hour:
            print("\n❌ Validation failed for the requested hour")
        else:
            print("\n❌ Validation failed for the full date")
        sys.exit(1)

    # Display summary
    show_summary()

    print("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║   ✅ TEST COMPLETED SUCCESSFULLY                                   ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)

    print("\n📚 Next steps:")
    print("  1. Check the data in PostgreSQL")
    print("  2. Enable the Airflow DAG for daily automation")
    print("  3. Configure monitoring and alerts")
    print()


if __name__ == "__main__":
    main()
