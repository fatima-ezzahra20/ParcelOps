"""
extract_events.py

Appelle l'API FastAPI (/delivery_events) et dépose le résultat dans la couche Bronze.
"""

import os
from datetime import datetime

import pandas as pd
import requests

from ingestion.utils import log_ingestion

API_URL = "http://localhost:8000/delivery_events"


def extract_events():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        events = data["events"]

        df = pd.DataFrame(events)
        ingested_at = datetime.now()
        df["ingested_at"] = ingested_at

        os.makedirs("data/bronze", exist_ok=True)
        timestamp_str = ingested_at.strftime("%Y%m%d_%H%M%S")
        output_path = f"data/bronze/events_{timestamp_str}.parquet"
        df.to_parquet(output_path, index=False)

        log_ingestion("delivery_events", len(df), status="SUCCESS")
        return df

    except Exception as e:
        log_ingestion("delivery_events", 0, status="FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    extract_events()