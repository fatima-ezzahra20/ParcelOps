"""
extract_deliveries.py (version incrémentale)

Ne récupère que les livraisons créées/modifiées depuis le dernier watermark.
"""

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from ingestion.utils import log_ingestion
from ingestion.watermark import get_watermark, set_watermark

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "parcelops_db")
DB_USER = os.getenv("DB_USER", "parcelops_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "parcelops_pass")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def extract_deliveries():
    try:
        engine = create_engine(DATABASE_URL)

        last_watermark = get_watermark("deliveries")
        if last_watermark:
            query = f"SELECT * FROM deliveries WHERE updated_at > '{last_watermark}'"
        else:
            query = "SELECT * FROM deliveries"  # premier run : tout récupérer

        df = pd.read_sql(query, engine)

        ingested_at = datetime.now()
        df["ingested_at"] = ingested_at

        os.makedirs("data/bronze", exist_ok=True)
        timestamp_str = ingested_at.strftime("%Y%m%d_%H%M%S")
        output_path = f"data/bronze/deliveries_{timestamp_str}.parquet"
        df.to_parquet(output_path, index=False)

        if len(df) > 0:
            new_watermark = df["updated_at"].max()
            set_watermark("deliveries", new_watermark)

        log_ingestion("deliveries", len(df), status="SUCCESS")
        return df

    except Exception as e:
        log_ingestion("deliveries", 0, status="FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    extract_deliveries()