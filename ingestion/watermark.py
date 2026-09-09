"""
watermark.py

Gère le "dernier point de traitement" (watermark) pour chaque source,
stocké dans un simple fichier JSON. Permet de ne récupérer que les
données nouvelles/modifiées à chaque exécution du pipeline.
"""

import json
import os

WATERMARK_FILE = "data/watermark.json"


def get_watermark(source_name: str):
    if not os.path.exists(WATERMARK_FILE):
        return None
    with open(WATERMARK_FILE, "r") as f:
        data = json.load(f)
    return data.get(source_name)


def set_watermark(source_name: str, timestamp: str):
    data = {}
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r") as f:
            data = json.load(f)

    data[source_name] = str(timestamp)

    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        json.dump(data, f, indent=2)