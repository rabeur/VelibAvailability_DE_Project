import requests
import pandas as pd
from datetime import datetime, timezone
import os
import sys
import pytz

JSON_EXPORT_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"

def ingest():
    paris_tz = pytz.timezone('Europe/Paris')
    now = datetime.now(paris_tz)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    try:
        # API call to get JSON data
        response = requests.get(JSON_EXPORT_URL, timeout=120)
        response.raise_for_status()
        data = response.json()

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
        base_path = f"/app/data_lake/bronze/test/velib/ingestion_date={date}/hour={hour}"
        os.makedirs(base_path, exist_ok=True)
        file_path = f"{base_path}/snapshot_{timestamp}.parquet"
        dataframe.to_parquet(file_path, index=False)

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

if __name__ == "__main__":
    ingest()