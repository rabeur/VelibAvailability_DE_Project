from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from io import BytesIO
import os

# DAG default configuration
default_args = {
    'owner': 'velib_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def ingest_velib_data(**context):
    """
    Velib data ingestion from Paris Open Data API, with metadata enrichment and partitioned storage in Parquet format.
    """
    import pytz

    PARQUET_EXPORT_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/parquet"

    paris_tz = pytz.timezone('Europe/Paris')
    now = datetime.now(paris_tz)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    print(f"[{timestamp}] Starting Vélib ingestion...")

    # Download data in Parquet format
    params = {'parquet_compression': 'snappy', 'timezone': 'CET'}
    response = requests.get(PARQUET_EXPORT_URL, params=params, timeout=120)
    response.raise_for_status()

    print(f"Downloaded {len(response.content) / 1024:.2f} KB")

    # Load into DataFrame
    dataframe = pd.read_parquet(BytesIO(response.content))

    # Add metadata columns
    dataframe["ingestion_timestamp"] = now.isoformat()
    dataframe["snapshot_id"] = timestamp

    # Save to local storage in partitioned format
    base_path = f"/opt/airflow/dags/../data_lake/bronze/velib/ingestion_date={date}/hour={hour}"
    os.makedirs(base_path, exist_ok=True)

    file_path = f"{base_path}/snapshot_{timestamp}.parquet"
    dataframe.to_parquet(file_path, index=False, compression='snappy')

    print(f"✅ Snapshot saved: {file_path}")
    print(f"📊 {len(dataframe)} stations ingested")

    # Push metrics to XCom pour monitoring
    context['ti'].xcom_push(key='num_stations', value=len(dataframe))
    context['ti'].xcom_push(key='file_path', value=file_path)

    return file_path

def validate_data(**context):
    """
    Basic validation checks on the ingested Vélib data, ensuring non-empty dataset and presence of key columns.
    """
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='ingest_velib', key='file_path')

    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"File not found: {file_path}")

    # Load data for validation
    dataframe = pd.read_parquet(file_path)

    # Quality checks
    assert len(dataframe) > 0, "Dataset is empty"
    assert 'stationcode' in dataframe.columns, "Missing stationcode column"

    print(f"✅ Validation passed: {len(dataframe)} records")
    return True

# DAG definition
with DAG(
    'velib_ingestion_pipeline',
    default_args=default_args,
    description='Real-time ingestion of Vélib station availability data with metadata enrichment and partitioned storage',
    schedule_interval='*/1 * * * *',  # every minute
    catchup=False,
    tags=['velib', 'ingestion', 'bronze'],
) as dag:

    # Task 1: Ingestion
    ingest_task = PythonOperator(
        task_id='ingest_velib',
        python_callable=ingest_velib_data,
        provide_context=True,
    )

    # Task 2: Validation
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )

    # Task 3: success notification
    success_task = BashOperator(
        task_id='notify_success',
        bash_command='echo "✅ Vélib ingestion completed successfully"',
    )

    # dependency chain
    ingest_task >> validate_task >> success_task