"""
extract_events.py (version incrémentale)

Utilise le paramètre ?since= de l'API pour ne récupérer que les événements
plus récents que le dernier watermark connu.
"""

import os
from datetime import datetime

import pandas as pd
import requests

from ingestion.utils import log_ingestion
from ingestion.watermark import get_watermark, set_watermark

API_URL = "http://localhost:8000/delivery_events"


def extract_events():
    try:
        last_watermark = get_watermark("events")
        params = {"since": last_watermark} if last_watermark else {}

        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        events = data["events"]

        df = pd.DataFrame(events)
        ingested_at = datetime.now()

        os.makedirs("data/bronze", exist_ok=True)
        timestamp_str = ingested_at.strftime("%Y%m%d_%H%M%S")
        output_path = f"data/bronze/events_{timestamp_str}.parquet"

        if len(df) > 0:
            df["ingested_at"] = ingested_at
            df.to_parquet(output_path, index=False)
            new_watermark = pd.to_datetime(
                df["status_timestamp"],
                format="ISO8601"
            ).max()
            set_watermark("events", new_watermark.isoformat())
        else:
            # Aucun nouvel événement : on écrit quand même un fichier vide
            # pour garder une trace de ce run (utile pour le monitoring)
            pd.DataFrame(columns=["event_id"]).to_parquet(output_path, index=False)

        log_ingestion("delivery_events", len(df), status="SUCCESS")
        return df

    except Exception as e:
        log_ingestion("delivery_events", 0, status="FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    extract_events()