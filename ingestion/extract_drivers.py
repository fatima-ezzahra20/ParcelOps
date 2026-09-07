"""
extract_drivers.py

Lit le fichier CSV des livreurs (simulant un export RH/planning)
et le dépose dans la couche Bronze.
"""

import os
from datetime import datetime

import pandas as pd

from ingestion.utils import log_ingestion

SOURCE_CSV = "data/bronze_raw_drivers.csv"


def extract_drivers():
    try:
        df = pd.read_csv(SOURCE_CSV)

        ingested_at = datetime.now()
        df["ingested_at"] = ingested_at

        os.makedirs("data/bronze", exist_ok=True)
        timestamp_str = ingested_at.strftime("%Y%m%d_%H%M%S")
        output_path = f"data/bronze/drivers_{timestamp_str}.parquet"
        df.to_parquet(output_path, index=False)

        log_ingestion("drivers", len(df), status="SUCCESS")
        return df

    except Exception as e:
        log_ingestion("drivers", 0, status="FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    extract_drivers()