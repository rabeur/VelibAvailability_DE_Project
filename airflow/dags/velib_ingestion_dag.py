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
    Ingestion Vélib --> Parquet Bronze avec schéma stable.
    - Dé-normalise coordonnees_geo -> latitude/longitude
    - Force les types (entiers nullable, float, string)
    - Écrit en Parquet avec schéma PyArrow explicite
    """
    import pytz, json
    from io import BytesIO
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    PARQUET_EXPORT_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/parquet"

    paris_tz = pytz.timezone('Europe/Paris')
    now = datetime.now(paris_tz)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    print(f"[{timestamp}] Starting Vélib ingestion...")

    # 1) Télécharger le snapshot parquet depuis l'API
    params = {'parquet_compression': 'snappy', 'timezone': 'CET'}
    resp = requests.get(PARQUET_EXPORT_URL, params=params, timeout=120)
    resp.raise_for_status()
    print(f"Downloaded {len(resp.content) / 1024:.2f} KB")

    # 2) Charger en DataFrame pandas (moteur pyarrow)
    df = pd.read_parquet(BytesIO(resp.content), engine='pyarrow')

    # 3) Dé-normaliser coordonnees_geo -> latitude / longitude
    #    La colonne peut être list/tuple, ndarray ou string JSON "[lat, lon]"
    def parse_coord(v):
        try:
            if isinstance(v, (list, tuple)) and len(v) == 2:
                return float(v[0]), float(v[1])
            if hasattr(v, "__array__"):  # numpy array
                vv = list(v)
                return float(vv[0]), float(vv[1])
            if isinstance(v, str):
                arr = json.loads(v)
                return float(arr[0]), float(arr[1])
        except Exception:
            pass
        return np.nan, np.nan

    if "coordonnees_geo" in df.columns:
        lat_lon = df["coordonnees_geo"].apply(parse_coord).apply(pd.Series)
        lat_lon.columns = ["latitude", "longitude"]
        df["latitude"] = pd.to_numeric(lat_lon["latitude"], errors="coerce").astype("float64")
        df["longitude"] = pd.to_numeric(lat_lon["longitude"], errors="coerce").astype("float64")
    else:
        df["latitude"] = np.nan
        df["longitude"] = np.nan

    # 4) Colonnes cibles & normalisation des types (nullable Int64 pour éviter les float "parasites")
    #    NB: pandas 'Int64' = entier nullable (sérialisé en int64 Arrow avec nulls)
    INT_COLS = ["capacity", "numbikesavailable", "mechanical", "ebike", "numdocksavailable"]
    for c in INT_COLS:
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Colonnes texte
    STR_COLS = [
        "stationcode", "name", "is_installed", "is_renting", "is_returning",
        "nom_arrondissement_communes", "code_insee_commune"
    ]
    for c in STR_COLS:
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = df[c].astype("string")

    # Timestamps & metadata
    df["ingestion_timestamp"] = now.isoformat()
    df["snapshot_id"] = timestamp
    df["ingestion_timestamp"] = df["ingestion_timestamp"].astype("string")
    df["snapshot_id"] = df["snapshot_id"].astype("string")

    # 5) Sélection / ordre final des colonnes
    expected_cols = [
        "stationcode", "name",
        "capacity", "numbikesavailable", "mechanical", "ebike", "numdocksavailable",
        "is_installed", "is_renting", "is_returning",
        "latitude", "longitude",
        "nom_arrondissement_communes", "code_insee_commune",
        "ingestion_timestamp", "snapshot_id"
    ]
    # Ajouter les colonnes manquantes si besoin
    for c in expected_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[expected_cols]

    # 6) Schéma Arrow explicite et écriture Parquet homogène
    schema = pa.schema([
        ("stationcode", pa.string()),
        ("name", pa.string()),
        ("capacity", pa.int64()),
        ("numbikesavailable", pa.int64()),
        ("mechanical", pa.int64()),
        ("ebike", pa.int64()),
        ("numdocksavailable", pa.int64()),
        ("is_installed", pa.string()),
        ("is_renting", pa.string()),
        ("is_returning", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("nom_arrondissement_communes", pa.string()),
        ("code_insee_commune", pa.string()),
        ("ingestion_timestamp", pa.string()),   # on reste en string; conversion en Silver
        ("snapshot_id", pa.string()),
    ])

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)

    base_path = f"/opt/airflow/dags/../data_lake/bronze/velib/ingestion_date={date}/hour={hour}"
    os.makedirs(base_path, exist_ok=True)
    file_path = f"{base_path}/snapshot_{timestamp}.parquet"

    pq.write_table(table, file_path, compression="snappy")

    print(f"✅ Snapshot saved: {file_path}")
    print(f"📊 {len(df)} stations ingested")

    # XCom monitoring
    context['ti'].xcom_push(key='num_stations', value=len(df))
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