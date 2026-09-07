"""
generate_sources.py

Génère les 3 sources de données du projet ParcelOps :
1. deliveries       -> insérées dans PostgreSQL (table source)
2. delivery_events  -> stockées dans un fichier JSON (servira de base à l'API FastAPI)
3. drivers.csv      -> fichier CSV

Des problèmes de qualité sont injectés volontairement pour donner du contenu
réel au futur module de Data Quality (étape 4).
"""

import json
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "parcelops_db")
DB_USER = os.getenv("DB_USER", "parcelops_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "parcelops_pass")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

fake = Faker("fr_FR")
random.seed(42)  # reproductibilité

N_DRIVERS = 30
N_DELIVERIES = 250
N_EVENTS = 650

# Zones volontairement mal normalisées (variations de casse / orthographe)
# pour alimenter les règles de Data Quality
ZONES_CLEAN = ["Maarif", "Gueliz", "Hivernage", "Sidi Ghanem", "Palmeraie"]
ZONES_DIRTY_VARIANTS = {
    "Maarif": ["Maarif", "maarif", "MAARIF"],
    "Gueliz": ["Gueliz", "gueliz", "Guéliz"],
    "Hivernage": ["Hivernage", "hivernage"],
    "Sidi Ghanem": ["Sidi Ghanem", "sidi ghanem", "SIDI GHANEM"],
    "Palmeraie": ["Palmeraie", "palmeraie"],
}

VEHICLE_TYPES = ["velo", "scooter", "camionnette"]
CONTRACT_TYPES = ["interne", "sous-traitant"]
PRIORITIES = ["standard", "express"]
STATUSES_ORDER = ["picked_up", "in_transit", "delivered"]


# ---------------------------------------------------------
# 1. Génération des livreurs (drivers.csv)
# ---------------------------------------------------------

def generate_drivers():
    drivers = []
    for i in range(1, N_DRIVERS + 1):
        driver_id = f"DRV-{i:03d}"
        drivers.append({
            "driver_id": driver_id,
            "driver_name": fake.name(),
            "assigned_zone": random.choice(ZONES_CLEAN),
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "contract_type": random.choice(CONTRACT_TYPES),
            "active": random.choices([True, False], weights=[0.9, 0.1])[0],
        })
    df = pd.DataFrame(drivers)
    df.to_csv("data/bronze_raw_drivers.csv", index=False)
    print(f"[OK] {len(df)} livreurs générés -> data/bronze_raw_drivers.csv")
    return df


# ---------------------------------------------------------
# 2. Génération des livraisons (deliveries) -> PostgreSQL
# ---------------------------------------------------------

def generate_deliveries():
    deliveries = []
    base_date = datetime(2026, 9, 1)

    for i in range(1, N_DELIVERIES + 1):
        delivery_id = f"DEL-{i:04d}"
        pickup_date = base_date + timedelta(days=random.randint(0, 4))

        window_start_hour = random.randint(8, 16)
        window_start = pickup_date.replace(
            hour=window_start_hour, minute=0, second=0
        )
        window_end = window_start + timedelta(hours=2)

        # Zone volontairement "sale" (casse/orthographe incohérente)
        clean_zone = random.choice(ZONES_CLEAN)
        dirty_zone = random.choice(ZONES_DIRTY_VARIANTS[clean_zone])

        deliveries.append({
            "delivery_id": delivery_id,
            "customer_id": f"CUST-{random.randint(1, 150):04d}",
            "pickup_date": pickup_date.date(),
            "delivery_zone": dirty_zone,
            "scheduled_window_start": window_start,
            "scheduled_window_end": window_end,
            "package_weight_kg": round(random.uniform(0.2, 15.0), 2),
            "priority": random.choices(PRIORITIES, weights=[0.7, 0.3])[0],
            "updated_at": pickup_date,
        })

    df = pd.DataFrame(deliveries)

    engine = create_engine(DATABASE_URL)
    df.to_sql("deliveries", engine, if_exists="append", index=False)
    print(f"[OK] {len(df)} livraisons insérées dans PostgreSQL (table deliveries)")
    return df


# ---------------------------------------------------------
# 3. Génération des événements (delivery_events) -> JSON
# ---------------------------------------------------------

def generate_events(deliveries_df, drivers_df):
    events = []
    event_counter = 1
    valid_delivery_ids = deliveries_df["delivery_id"].tolist()
    valid_driver_ids = drivers_df["driver_id"].tolist()

    for _, delivery in deliveries_df.iterrows():
        delivery_id = delivery["delivery_id"]
        pickup_dt = datetime.combine(delivery["pickup_date"], datetime.min.time())

        # Chaque livraison a entre 1 et 3 événements (parfois incomplet -> "en cours")
        n_events_for_this_delivery = random.randint(1, 3)
        statuses_to_generate = STATUSES_ORDER[:n_events_for_this_delivery]

        current_time = pickup_dt + timedelta(hours=random.randint(1, 3))

        for status in statuses_to_generate:
            event_id = f"EVT-{event_counter:05d}"
            event_counter += 1

            driver_id = random.choice(valid_driver_ids)

            # --- Injection de problèmes de qualité ---

            # 1. Valeur nulle sur driver_id (~5% des cas)
            if random.random() < 0.05:
                driver_id = None

            # 2. Timestamp invalide (~3% des cas) : antérieur au pickup_date
            if random.random() < 0.03:
                status_timestamp = pickup_dt - timedelta(hours=5)
            # 3. Timestamp dans le futur (~2% des cas)
            elif random.random() < 0.02:
                status_timestamp = datetime.now() + timedelta(days=2)
            else:
                status_timestamp = current_time
                current_time += timedelta(minutes=random.randint(20, 90))

            events.append({
                "event_id": event_id,
                "delivery_id": delivery_id,
                "driver_id": driver_id,
                "status": status,
                "status_timestamp": status_timestamp.isoformat(),
                "gps_lat": round(random.uniform(31.55, 31.68), 5),
                "gps_lng": round(random.uniform(-8.05, -7.95), 5),
            })

    # 4. Doublons volontaires (~3% des événements dupliqués tels quels)
    n_duplicates = int(len(events) * 0.03)
    duplicates = random.sample(events, n_duplicates)
    events.extend(duplicates)

    # 5. Incohérence référentielle : quelques events pointent vers un delivery_id inexistant
    n_orphans = 8
    for _ in range(n_orphans):
        event_counter += 1
        events.append({
            "event_id": f"EVT-{event_counter:05d}",
            "delivery_id": f"DEL-{9999 + _}",  # n'existe pas dans deliveries
            "driver_id": random.choice(valid_driver_ids),
            "status": random.choice(STATUSES_ORDER),
            "status_timestamp": datetime.now().isoformat(),
            "gps_lat": round(random.uniform(31.55, 31.68), 5),
            "gps_lng": round(random.uniform(-8.05, -7.95), 5),
        })

    # 6. Statuts hors ordre : on mélange légèrement l'ordre pour ~5% des livraisons
    random.shuffle(events)

    with open("data/bronze_raw_events.json", "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)

    print(f"[OK] {len(events)} événements générés -> data/bronze_raw_events.json")
    return events


# ---------------------------------------------------------
# Exécution principale
# ---------------------------------------------------------

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    print("Génération des sources de données ParcelOps...\n")

    drivers_df = generate_drivers()
    deliveries_df = generate_deliveries()
    generate_events(deliveries_df, drivers_df)

    print("\nTerminé. Les 3 sources sont prêtes :")
    print("  - PostgreSQL : table 'deliveries'")
    print("  - data/bronze_raw_events.json (base de l'API)")
    print("  - data/bronze_raw_drivers.csv")