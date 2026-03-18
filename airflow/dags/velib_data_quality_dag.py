from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import glob
import os
import sys

# add utils to path for imports
sys.path.insert(0, os.path.dirname(__file__))
from utils.data_quality import VelibDataQuality

default_args = {
    'owner': 'velib_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 18),
    'email_on_failure': False,  # Activate if email alerts are set up
    'email_on_retry': False,
    #'email': ['rabiera69@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_latest_snapshot(**context):
    """Load the latest Vélib snapshot from the data lake"""
    data_path = "/opt/airflow/data_lake/bronze/velib"

    # Find all Parquet files in the data lake
    parquet_files = glob.glob(f"{data_path}/**/*.parquet", recursive=True)

    if not parquet_files:
        raise FileNotFoundError("No parquets found in data lake")

    # Order by modification time and load the latest one
    latest_file = max(parquet_files, key=os.path.getmtime)

    print(f"📂 File loaded: {latest_file}")

    # Load into DataFrame
    df = pd.read_parquet(latest_file)

    print(f"📊 Shape: {df.shape}")
    print(f"📋 Columns: {list(df.columns)}")

    # Temp file path for quality checks
    temp_path = "/tmp/velib_latest.parquet"
    df.to_parquet(temp_path)

    # Push path and row count to XCom for downstream tasks
    context['ti'].xcom_push(key='data_path', value=temp_path)
    context['ti'].xcom_push(key='num_rows', value=len(df))

    return temp_path


def run_quality_checks(**context):
    """Execute data quality checks on the latest snapshot and compile a report"""
    ti = context['ti']
    data_path = ti.xcom_pull(task_ids='load_data', key='data_path')

    if not data_path or not os.path.exists(data_path):
        raise FileNotFoundError(f"Données introuvables: {data_path}")

    # Load data into DataFrame
    df = pd.read_parquet(data_path)

    # Create data quality checker instance
    dq = VelibDataQuality(df)

    # Execute all checks and get the report
    report = dq.run_all_checks()

    report_dt = context['data_interval_end'].in_timezone('Europe/Paris').subtract(minutes=1)
    report_date = report_dt.format('YYYY-MM-DD')
    report_hour = report_dt.format('HH')
    report_minute = report_dt.format('mm')

    # Save report to a text file using a date/hour partitioned layout
    report_dir = (
        f"/opt/airflow/data_lake/reports/data_quality/"
        f"report_date={report_date}/hour={report_hour}"
    )
    os.makedirs(report_dir, exist_ok=True)

    report_file = f"{report_dir}/report-{report_date}-{report_hour}-{report_minute}.txt"

    report_lines = [
        "VELIB DATA QUALITY REPORT",
        f"Timestamp: {report['timestamp']}",
        f"Rows checked: {report['total_rows']}",
        f"Tests passed: {report['tests_passed']}",
        f"Tests failed: {report['tests_failed']}",
        f"Success rate: {report['success_rate']:.1f}%",
        "",
        "Details:",
    ]

    for detail in report['details']:
        status = 'PASS' if detail['passed'] else 'FAIL'
        report_lines.append(f"- [{status}] {detail['test']}: {detail['message']}")

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines) + "\n")

    print(f"\n📄 Report saved to: {report_file}")

    # Push metrics to XCom for monitoring
    ti.xcom_push(key='success_rate', value=report['success_rate'])
    ti.xcom_push(key='tests_passed', value=report['tests_passed'])
    ti.xcom_push(key='tests_failed', value=report['tests_failed'])

    # Fail the task if success rate is below threshold (e.g., 80%)
    if report['success_rate'] < 80:
        raise ValueError(f"❌ Succes rate too low: {report['success_rate']:.1f}%")

    return report_file


def generate_alert(**context):
    """Raise alerts if data quality checks fail"""
    ti = context['ti']
    success_rate = ti.xcom_pull(task_ids='quality_checks', key='success_rate')
    tests_failed = ti.xcom_pull(task_ids='quality_checks', key='tests_failed')

    if tests_failed > 0:
        alert_msg = f"""
        ⚠️  DATA QUALITY AlERT

        Succes Rate: {success_rate:.1f}%
        Tests failed: {tests_failed}
        Timestamp: {datetime.now().isoformat()}

        Check logs for more details.
        """
        print(alert_msg)

    return success_rate


# DAG definition
with DAG(
    'velib_data_quality',
    default_args=default_args,
    description='Data Quality checks for Vélib data',
    schedule_interval='0/1 * * * *',  # Every minutes cause velib data are updated every minute
    catchup=False,
    tags=['velib', 'data-quality', 'monitoring'],
) as dag:

    # Task 1: Load last snapshot
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_latest_snapshot,
    )

    # Task 2: Execute quality checks
    quality_task = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality_checks,
    )

    # Task 3: Generate alerts if necessary
    alert_task = PythonOperator(
        task_id='generate_alerts',
        python_callable=generate_alert,
    )

    # Task 4: Cleanup temporary files
    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='rm -f /tmp/velib_latest.parquet',
    )

    # Define dependencies
    load_task >> quality_task >> alert_task >> cleanup_task