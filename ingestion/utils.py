"""
utils.py

Fonction partagée pour logger chaque ingestion dans un fichier CSV de suivi.
Ça permettra plus tard de répondre à "le pipeline a-t-il bien tourné ?"
"""

import os
import csv
from datetime import datetime

LOG_FILE = "data/bronze/_ingestion_log.csv"


def log_ingestion(source_name: str, record_count: int, status: str = "SUCCESS", error_message: str = ""):
    os.makedirs("data/bronze", exist_ok=True)
    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "source_name", "record_count", "status", "error_message"])
        writer.writerow([datetime.now().isoformat(), source_name, record_count, status, error_message])

    print(f"[{status}] {source_name} : {record_count} lignes ingérées")