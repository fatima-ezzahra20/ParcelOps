"""
to_silver.py

Prend les données VALIDÉES (data/silver/xxx_valid.parquet, sorties de l'étape
Data Quality) et applique la normalisation finale :
  - uniformisation des zones (casse, accents, espaces)
  - vérification des types

Résultat : data/silver/xxx_silver.parquet -> prêt pour la modélisation Gold (étape 6).
"""

import pandas as pd

# Mapping de normalisation des zones : toute variante connue -> nom canonique
ZONE_NORMALIZATION = {
    "maarif": "Maarif",
    "gueliz": "Gueliz",
    "guéliz": "Gueliz",
    "hivernage": "Hivernage",
    "sidi ghanem": "Sidi Ghanem",
    "palmeraie": "Palmeraie",
}


def normalize_zone(zone: str) -> str:
    if pd.isna(zone):
        return zone
    key = zone.strip().lower()
    return ZONE_NORMALIZATION.get(key, zone.strip())


def transform_deliveries():
    df = pd.read_parquet("data/silver/deliveries_valid.parquet")

    df["delivery_zone"] = df["delivery_zone"].apply(normalize_zone)

    # Vérification des types (au cas où)
    df["pickup_date"] = pd.to_datetime(df["pickup_date"])
    df["scheduled_window_start"] = pd.to_datetime(df["scheduled_window_start"])
    df["scheduled_window_end"] = pd.to_datetime(df["scheduled_window_end"])
    df["package_weight_kg"] = df["package_weight_kg"].astype(float)

    df.to_parquet("data/silver/deliveries_silver.parquet", index=False)
    print(f"[OK] deliveries : {len(df)} lignes normalisées -> deliveries_silver.parquet")
    print(f"     Zones uniques après normalisation : {sorted(df['delivery_zone'].unique())}")
    return df


def transform_drivers():
    df = pd.read_parquet("data/silver/drivers_valid.parquet")

    df["assigned_zone"] = df["assigned_zone"].apply(normalize_zone)

    df.to_parquet("data/silver/drivers_silver.parquet", index=False)
    print(f"[OK] drivers : {len(df)} lignes normalisées -> drivers_silver.parquet")
    return df


def transform_events():
    df = pd.read_parquet("data/silver/events_valid.parquet")

    df["status_timestamp"] = pd.to_datetime(df["status_timestamp"])

    df.to_parquet("data/silver/events_silver.parquet", index=False)
    print(f"[OK] events : {len(df)} lignes normalisées -> events_silver.parquet")
    return df


if __name__ == "__main__":
    print("Transformation Silver en cours...\n")

    transform_deliveries()
    transform_drivers()
    transform_events()

    print("\nTerminé. Les 3 sources sont propres et prêtes pour la modélisation Gold.")