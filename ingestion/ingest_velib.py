import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timezone
import os
import sys
import pytz



PARQUET_EXPORT_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/parquet"

def ingest():
    paris_tz = pytz.timezone('Europe/Paris')
    now = datetime.now(paris_tz)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    try:
        params = {
            'parquet_compression': 'snappy',
            'timezone': 'UTC'
        }
        # API call with timeout and error handling
        response = requests.get(PARQUET_EXPORT_URL, params=params, timeout=120)
        response.raise_for_status()
        parquet_buffer = BytesIO(response.content)
        # Create DataFrame
        dataframe = pd.read_parquet(parquet_buffer)
        dataframe["ingestion_timestamp"] = now.isoformat()
        dataframe["snapshot_id"] = timestamp
        # save to parquet
        base_path = f"/app/data_lake/bronze/velib/ingestion_date={date}/hour={hour}"
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