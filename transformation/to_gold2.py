"""
to_gold.py (version incrémentale avec upsert)

Construit/actualise le Star Schema Gold à partir des données Silver.

Différence par rapport à la version précédente :
  - Les dimensions n'insèrent que les valeurs NOUVELLES (pas déjà présentes)
  - fact_deliveries utilise un UPSERT (INSERT ... ON CONFLICT DO UPDATE)
    pour mettre à jour une livraison déjà connue plutôt que la dupliquer
"""

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "parcelops_db")
DB_USER = os.getenv("DB_USER", "parcelops_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "parcelops_pass")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def upsert_dim_zone(deliveries_df, drivers_df, engine):
    zones = pd.concat([
        deliveries_df["delivery_zone"],
        drivers_df["assigned_zone"],
    ]).dropna().unique()

    existing = pd.read_sql("SELECT zone_name FROM dim_zone", engine)["zone_name"].tolist()
    new_zones = [z for z in zones if z not in existing]

    if new_zones:
        pd.DataFrame({"zone_name": sorted(new_zones)}).to_sql(
            "dim_zone", engine, if_exists="append", index=False
        )

    print(f"[OK] dim_zone : {len(new_zones)} nouvelles zones ajoutées")
    return pd.read_sql("SELECT * FROM dim_zone", engine)


def upsert_dim_driver(drivers_df, engine):
    existing = pd.read_sql("SELECT driver_id FROM dim_driver", engine)["driver_id"].tolist()
    new_drivers = drivers_df[~drivers_df["driver_id"].isin(existing)]

    if len(new_drivers) > 0:
        new_drivers[["driver_id", "driver_name", "vehicle_type", "contract_type", "active"]].to_sql(
            "dim_driver", engine, if_exists="append", index=False
        )

    print(f"[OK] dim_driver : {len(new_drivers)} nouveaux livreurs ajoutés")
    return pd.read_sql("SELECT * FROM dim_driver", engine)


def upsert_dim_date(deliveries_df, engine):
    dates = pd.to_datetime(deliveries_df["pickup_date"]).dt.normalize().unique()

    existing = pd.read_sql("SELECT date_key FROM dim_date", engine)["date_key"].tolist()

    rows = []
    for d in dates:
        d = pd.Timestamp(d)
        date_key = int(d.strftime("%Y%m%d"))
        if date_key in existing:
            continue
        rows.append({
            "date_key": date_key,
            "full_date": d.date(),
            "day": d.day,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "day_of_week": d.strftime("%A"),
            "is_weekend": d.dayofweek >= 5,
        })

    if rows:
        pd.DataFrame(rows).to_sql("dim_date", engine, if_exists="append", index=False)

    print(f"[OK] dim_date : {len(rows)} nouvelles dates ajoutées")
    return pd.read_sql("SELECT * FROM dim_date", engine)


def compute_last_status(events_df: pd.DataFrame) -> pd.DataFrame:
    if len(events_df) == 0:
        return pd.DataFrame(columns=["delivery_id", "driver_id", "last_status", "last_status_timestamp"])
    events_sorted = events_df.sort_values("status_timestamp")
    last_events = events_sorted.groupby("delivery_id", as_index=False).last()
    return last_events[["delivery_id", "driver_id", "status", "status_timestamp"]].rename(
        columns={"status": "last_status", "status_timestamp": "last_status_timestamp"}
    )


def upsert_fact_deliveries(deliveries_df, drivers_dim, zone_dim, last_status_df, engine):
    df = deliveries_df.merge(last_status_df, on="delivery_id", how="left")

    now = pd.Timestamp(datetime.now())
    df["is_late_now"] = (now > df["scheduled_window_end"]) & (df["last_status"] != "delivered")
    df["was_delivered_late"] = (df["last_status"] == "delivered") & (
        df["last_status_timestamp"] > df["scheduled_window_end"]
    )

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

    # UPSERT : on insère, et en cas de conflit sur delivery_id (déjà existant),
    # on met à jour les colonnes plutôt que de dupliquer la ligne
    upsert_query = text("""
        INSERT INTO fact_deliveries (
            delivery_id, zone_key, driver_key, date_key,
            package_weight_kg, priority,
            scheduled_window_start, scheduled_window_end,
            last_status, last_status_timestamp,
            is_late_now, was_delivered_late
        ) VALUES (
            :delivery_id, :zone_key, :driver_key, :date_key,
            :package_weight_kg, :priority,
            :scheduled_window_start, :scheduled_window_end,
            :last_status, :last_status_timestamp,
            :is_late_now, :was_delivered_late
        )
        ON CONFLICT (delivery_id) DO UPDATE SET
            zone_key = EXCLUDED.zone_key,
            driver_key = EXCLUDED.driver_key,
            date_key = EXCLUDED.date_key,
            last_status = EXCLUDED.last_status,
            last_status_timestamp = EXCLUDED.last_status_timestamp,
            is_late_now = EXCLUDED.is_late_now,
            was_delivered_late = EXCLUDED.was_delivered_late,
            loaded_at = NOW()
    """)

    records = fact.to_dict(orient="records")
    # NaN -> None pour que psycopg2 les traite comme NULL
    for r in records:
        for k, v in r.items():
            if pd.isna(v):
                r[k] = None

    with engine.begin() as conn:
        for record in records:
            conn.execute(upsert_query, record)

    print(f"[OK] fact_deliveries : {len(records)} lignes upsertées")
    n_late_now = fact["is_late_now"].sum()
    n_delivered_late = fact["was_delivered_late"].sum()
    print(f"     -> {n_late_now} livraisons en retard actuellement")
    print(f"     -> {n_delivered_late} livraisons livrées en retard")


if __name__ == "__main__":
    print("Actualisation du Star Schema Gold (incremental)...\n")

    engine = create_engine(DATABASE_URL)

    deliveries_df = pd.read_parquet("data/silver/deliveries_silver.parquet")
    drivers_df = pd.read_parquet("data/silver/drivers_silver.parquet")
    events_df = pd.read_parquet("data/silver/events_silver.parquet")

    zone_dim = upsert_dim_zone(deliveries_df, drivers_df, engine)
    drivers_dim = upsert_dim_driver(drivers_df, engine)
    upsert_dim_date(deliveries_df, engine)

    last_status_df = compute_last_status(events_df)

    upsert_fact_deliveries(deliveries_df, drivers_dim, zone_dim, last_status_df, engine)

    print("\nTerminé.")