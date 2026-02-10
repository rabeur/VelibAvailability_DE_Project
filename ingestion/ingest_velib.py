import requests
import pandas as pd
from datetime import datetime, timezone
import os

API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records"

def ingest():
    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    ts = now.strftime("%Y%m%d_%H%M%S")

    params = {"limit": 5}
    r = requests.get(API_URL, params=params)
    data = r.json()["results"]

    df = pd.DataFrame(data)
    df["ingestion_timestamp"] = now.isoformat()
    df["snapshot_id"] = ts

    base_path = f"/app/data_lake/bronze/velib/ingestion_date={date}/hour={hour}"
    os.makedirs(base_path, exist_ok=True)

    file_path = f"{base_path}/snapshot_{ts}.parquet"
    df.to_parquet(file_path, index=False)

    print(f"Snapshot saved: {file_path}")

if __name__ == "__main__":
    ingest()