from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import pytz
import time

# DAG default configuration
default_args = {
    'owner': 'velib_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
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
        # Download JSON snapshot with retries because public API responses can be intermittently truncated.
        max_fetch_attempts = 2
        data = None
        resp = None

        for attempt in range(1, max_fetch_attempts + 1):
            try:
                resp = requests.get(
                    JSON_EXPORT_URL,
                    timeout=(5, 15),
                    headers={"Accept": "application/json"},
                )
                resp.raise_for_status()

                content_type = resp.headers.get("Content-Type", "unknown")
                body = resp.text

                if not body.strip():
                    raise ValueError("Empty response body")

                try:
                    data = resp.json()
                except ValueError as json_error:
                    preview = body[:200].replace("\n", " ")
                    print(
                        f"Attempt {attempt}/{max_fetch_attempts}: invalid JSON response "
                        f"(status={resp.status_code}, content_type={content_type}, size={len(resp.content)} bytes)."
                    )
                    print(f"Response preview: {preview}")
                    if attempt < max_fetch_attempts:
                        time.sleep(1)
                        continue
                    raise ValueError(f"Invalid JSON from API after {max_fetch_attempts} attempts: {json_error}") from json_error

                print(
                    f"Downloaded {len(resp.content) / 1024:.2f} KB "
                    f"(status={resp.status_code}, content_type={content_type})"
                )
                break

            except requests.exceptions.RequestException as request_error:
                print(f"Attempt {attempt}/{max_fetch_attempts}: network error: {request_error}")
                if attempt < max_fetch_attempts:
                    time.sleep(1)
                    continue
                raise RuntimeError(
                    f"Failed to download Velib data after {max_fetch_attempts} attempts"
                ) from request_error

        if data is None:
            raise RuntimeError("No data received from API after retries")

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

        # Save to Parquet — atomic write to avoid corrupt files on interruption.
        # Write to a .tmp file first, then rename (rename is atomic on the filesystem).
        # If writing is interrupted, the .tmp file is discarded and the final path never exists.
        # One internal retry on write failure (e.g. transient I/O error) to avoid losing
        # a valid snapshot without adding an Airflow-level retry that would exceed the minute.
        base_path = f"/opt/airflow/dags/../data_lake/bronze/velib/ingestion_date={date}/hour={hour}"
        os.makedirs(base_path, exist_ok=True)
        file_path = f"{base_path}/snapshot_{timestamp}.parquet"
        tmp_path = file_path + ".tmp"
        max_write_attempts = 2
        for write_attempt in range(1, max_write_attempts + 1):
            try:
                dataframe.to_parquet(tmp_path, index=False)
                os.rename(tmp_path, file_path)
                break
            except Exception as write_error:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                if write_attempt < max_write_attempts:
                    print(f"Write attempt {write_attempt}/{max_write_attempts} failed: {write_error}. Retrying...")
                    time.sleep(2)
                else:
                    raise

        print(f"✅ Snapshot saved: {file_path}")
        print(f"📊 {len(dataframe)} stations ingested")

        # XCom monitoring
        context['ti'].xcom_push(key='num_stations', value=len(dataframe))
        context['ti'].xcom_push(key='file_path', value=file_path)
        return file_path

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        raise
    except KeyError as e:
        print(f"❌ KeyError: {e}")
        raise
    except ValueError as e:
        print(f"❌ Data/JSON error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise

def validate_data(**context):
    """
    Basic validation checks on the ingested Vélib data, ensuring non-empty dataset and presence of key columns.
    """
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='ingest_velib', key='file_path')

    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"File not found: {file_path}")

    # Load data for validation
    try:
        dataframe = pd.read_parquet(file_path)
    except OSError as e:
        raise ValueError(
            f"Parquet file is corrupted or incomplete: {file_path}. "
            f"This usually means the ingestion write was interrupted. Details: {e}"
        ) from e

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
    max_active_runs=1,
    dagrun_timeout=timedelta(seconds=50),
    tags=['velib', 'ingestion', 'bronze'],
) as dag:

    # Task 1: Ingestion
    ingest_task = PythonOperator(
        task_id='ingest_velib',
        python_callable=ingest_velib_data,
        provide_context=True,
        execution_timeout=timedelta(seconds=40),
    )

    # Task 2: Validation
    # retries=1 here only: validate is fast (<5s) so one retry + 5s delay fits within the minute
    # without conflicting with default_args retries=0 set on the heavier ingest task.
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
        execution_timeout=timedelta(seconds=20),
        retries=1,
        retry_delay=timedelta(seconds=5),
    )

    # Task 3: success notification
    success_task = BashOperator(
        task_id='notify_success',
        bash_command='echo "✅ Vélib ingestion completed successfully"',
    )

    # dependency chain
    ingest_task >> validate_task >> success_task