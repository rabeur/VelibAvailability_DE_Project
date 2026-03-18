from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import sys
import pytz

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
    Ingestion Velib --> Parquet Bronze with stable schema.
    - De-normalizes coordonnees_geo -> latitude/longitude
    - Enforces types (nullable integers, floats, strings)
    - Writes Parquet with explicit schema
    """

    JSON_EXPORT_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"

    paris_tz = pytz.timezone('Europe/Paris')
    now = datetime.now(paris_tz)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    print(f"[{timestamp}] Starting Vélib ingestion...")
    try:
        # Download JSON API snapshot with timeout and error handling
        resp = requests.get(JSON_EXPORT_URL, timeout=120)
        resp.raise_for_status()
        print(f"Downloaded {len(resp.content) / 1024:.2f} KB")
        data = resp.json()

        # Extract the results list - handle if data is dict or list
        if isinstance(data, dict):
            stations = data.get('results', [])
        elif isinstance(data, list):
            stations = data
        else:
            raise ValueError("Unexpected JSON structure")

        # Create DataFrame
        dataframe = pd.DataFrame(stations)

        # Flatten geographical coordinates
        if 'coordonnees_geo' in dataframe.columns:
            dataframe['lon'] = dataframe['coordonnees_geo'].apply(lambda x: x.get('lon') if isinstance(x, dict) else None)
            dataframe['lat'] = dataframe['coordonnees_geo'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
            dataframe.drop(columns=['coordonnees_geo'], inplace=True)

        # Add ingestion metadata
        dataframe["ingestion_timestamp"] = now.isoformat()
        dataframe["snapshot_id"] = timestamp

        # Define and enforce data types for stability
        dtype_mapping = {
            'stationcode': 'string',
            'name': 'string',
            'is_installed': 'string',
            'capacity': 'Int64',  # Nullable integer
            'numdocksavailable': 'Int64',
            'numbikesavailable': 'Int64',
            'mechanical': 'Int64',
            'ebike': 'Int64',
            'is_renting': 'string',
            'is_returning': 'string',
            'duedate': 'string',  # Keep as string for now, could convert to datetime if needed
            'nom_arrondissement_communes': 'string',
            'code_insee_commune': 'string',
            'lon': 'float64',
            'lat': 'float64',
            'station_opening_hours': 'string',
            'ingestion_timestamp': 'string',
            'snapshot_id': 'string'
        }

        # Apply dtypes, ignoring any missing columns
        for col, dtype in dtype_mapping.items():
            if col in dataframe.columns:
                dataframe[col] = dataframe[col].astype(dtype)

        # Save to Parquet
        base_path = f"/opt/airflow/dags/../data_lake/bronze/velib/ingestion_date={date}/hour={hour}"
        os.makedirs(base_path, exist_ok=True)
        file_path = f"{base_path}/snapshot_{timestamp}.parquet"
        dataframe.to_parquet(file_path, index=False)

        print(f"✅ Snapshot saved: {file_path}")
        print(f"📊 {len(dataframe)} stations ingested")

        # XCom monitoring
        context['ti'].xcom_push(key='num_stations', value=len(dataframe))
        context['ti'].xcom_push(key='file_path', value=file_path)
        return file_path

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"❌ KeyError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

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