"""
api_events.py

Petite API FastAPI qui simule l'application mobile des livreurs.
Elle sert les événements de livraison générés dans data/bronze_raw_events.json.

Lancement :
    uvicorn data_generation.api_events:app --reload --port 8000

Endpoints :
    GET /delivery_events              -> tous les événements
    GET /delivery_events?since=...    -> événements après un timestamp donné
                                          (utile pour l'incremental loading, étape 7)
"""

import json
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query

app = FastAPI(title="ParcelOps - Delivery Events API")

EVENTS_FILE = "data/bronze_raw_events.json"


def load_events():
    with open(EVENTS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/")
def root():
    return {"message": "ParcelOps Delivery Events API - voir /docs pour la documentation"}


@app.get("/delivery_events")
def get_delivery_events(since: Optional[str] = Query(None, description="Format ISO, ex: 2026-09-01T00:00:00")):
    events = load_events()

    if since:
        since_dt = datetime.fromisoformat(since)
        events = [
            e for e in events
            if e["status_timestamp"] and datetime.fromisoformat(e["status_timestamp"]) > since_dt
        ]

    return {"count": len(events), "events": events}