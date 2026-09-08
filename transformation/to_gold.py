"""
to_gold.py

Construit le Star Schema Gold à partir des données Silver :
  - dim_zone, dim_driver, dim_date (dimensions avec clés de substitution)
  - fact_deliveries (table de faits, avec calcul du dernier statut connu
    et des indicateurs de retard)

Charge le résultat dans PostgreSQL.
"""

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "parcelops_db")
DB_USER = os.getenv("DB_USER", "parcelops_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "parcelops_pass")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def build_dim_zone(deliveries_df, drivers_df, engine):
    zones = pd.concat([
        deliveries_df["delivery_zone"],
        drivers_df["assigned_zone"],
    ]).dropna().unique()

    dim_zone = pd.DataFrame({"zone_name": sorted(zones)})
    dim_zone.to_sql("dim_zone", engine, if_exists="append", index=False)

    # On relit depuis Postgres pour récupérer les zone_key générées (SERIAL)
    return pd.read_sql("SELECT * FROM dim_zone", engine)


def build_dim_driver(drivers_df, engine):
    dim_driver = drivers_df[["driver_id", "driver_name", "vehicle_type", "contract_type", "active"]].copy()
    dim_driver.to_sql("dim_driver", engine, if_exists="append", index=False)

    return pd.read_sql("SELECT * FROM dim_driver", engine)


def build_dim_date(deliveries_df, engine):
    dates = pd.to_datetime(deliveries_df["pickup_date"]).dt.normalize().unique()

    rows = []
    for d in dates:
        d = pd.Timestamp(d)
        rows.append({
            "date_key": int(d.strftime("%Y%m%d")),
            "full_date": d.date(),
            "day": d.day,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "day_of_week": d.strftime("%A"),
            "is_weekend": d.dayofweek >= 5,
        })

    dim_date = pd.DataFrame(rows)
    dim_date.to_sql("dim_date", engine, if_exists="append", index=False)

    return pd.read_sql("SELECT * FROM dim_date", engine)


def compute_last_status(events_df: pd.DataFrame) -> pd.DataFrame:
    """Pour chaque delivery_id, garde uniquement l'événement le plus récent."""
    events_sorted = events_df.sort_values("status_timestamp")
    last_events = events_sorted.groupby("delivery_id", as_index=False).last()
    return last_events[["delivery_id", "driver_id", "status", "status_timestamp"]].rename(
        columns={"status": "last_status", "status_timestamp": "last_status_timestamp"}
    )


def build_fact_deliveries(deliveries_df, drivers_dim, zone_dim, date_dim, last_status_df, engine):
    df = deliveries_df.merge(last_status_df, on="delivery_id", how="left")

    now = pd.Timestamp(datetime.now())

    # Indicateur 1 : en retard MAINTENANT (toujours en cours, créneau dépassé)
    df["is_late_now"] = (now > df["scheduled_window_end"]) & (df["last_status"] != "delivered")

    # Indicateur 2 : a été livré, mais après le créneau prévu
    df["was_delivered_late"] = (df["last_status"] == "delivered") & (
        df["last_status_timestamp"] > df["scheduled_window_end"]
    )

    # Jointure avec les dimensions pour récupérer les clés de substitution
    df = df.merge(zone_dim, left_on="delivery_zone", right_on="zone_name", how="left")
    df = df.merge(drivers_dim, on="driver_id", how="left")

    df["date_key"] = pd.to_datetime(df["pickup_date"]).dt.strftime("%Y%m%d").astype(int)

    fact = df[[
        "delivery_id", "zone_key", "driver_key", "date_key",
        "package_weight_kg", "priority",
        "scheduled_window_start", "scheduled_window_end",
        "last_status", "last_status_timestamp",
        "is_late_now", "was_delivered_late",
    ]]

    fact.to_sql("fact_deliveries", engine, if_exists="append", index=False)
    print(f"[OK] fact_deliveries : {len(fact)} lignes chargées")

    n_late_now = fact["is_late_now"].sum()
    n_delivered_late = fact["was_delivered_late"].sum()
    print(f"     -> {n_late_now} livraisons en retard actuellement")
    print(f"     -> {n_delivered_late} livraisons livrées en retard")


if __name__ == "__main__":
    print("Construction du Star Schema Gold...\n")

    engine = create_engine(DATABASE_URL)

    deliveries_df = pd.read_parquet("data/silver/deliveries_silver.parquet")
    drivers_df = pd.read_parquet("data/silver/drivers_silver.parquet")
    events_df = pd.read_parquet("data/silver/events_silver.parquet")

    zone_dim = build_dim_zone(deliveries_df, drivers_df, engine)
    print(f"[OK] dim_zone : {len(zone_dim)} zones")

    drivers_dim = build_dim_driver(drivers_df, engine)
    print(f"[OK] dim_driver : {len(drivers_dim)} livreurs")

    date_dim = build_dim_date(deliveries_df, engine)
    print(f"[OK] dim_date : {len(date_dim)} dates")

    last_status_df = compute_last_status(events_df)

    build_fact_deliveries(deliveries_df, drivers_dim, zone_dim, date_dim, last_status_df, engine)

    print("\nTerminé. Le Star Schema est prêt dans PostgreSQL.")