"""
rules.py

Chaque fonction représente une règle de qualité individuelle.
Convention : chaque fonction prend un(des) DataFrame(s) et retourne
un masque booléen (True = ligne valide pour cette règle précise).
"""

import pandas as pd
from datetime import datetime


# ---------------------------------------------------------
# Règles sur DELIVERIES
# ---------------------------------------------------------

def rule_delivery_id_not_null(df: pd.DataFrame) -> pd.Series:
    return df["delivery_id"].notna()


def rule_delivery_id_unique(df: pd.DataFrame) -> pd.Series:
    return ~df["delivery_id"].duplicated(keep="first")


def rule_customer_id_not_null(df: pd.DataFrame) -> pd.Series:
    return df["customer_id"].notna()


def rule_valid_window(df: pd.DataFrame) -> pd.Series:
    # La fin du créneau doit être après le début
    return df["scheduled_window_end"] > df["scheduled_window_start"]


def rule_weight_positive(df: pd.DataFrame) -> pd.Series:
    return df["package_weight_kg"] > 0


# ---------------------------------------------------------
# Règles sur DELIVERY_EVENTS
# ---------------------------------------------------------

def rule_event_id_not_null(df: pd.DataFrame) -> pd.Series:
    return df["event_id"].notna()


def rule_event_not_duplicate(df: pd.DataFrame) -> pd.Series:
    return ~df.duplicated(subset=["event_id"], keep="first")


def rule_valid_status(df: pd.DataFrame) -> pd.Series:
    allowed = {"picked_up", "in_transit", "delivered", "failed"}
    return df["status"].isin(allowed)


def rule_timestamp_not_future(df: pd.DataFrame) -> pd.Series:
    now = pd.Timestamp(datetime.now())
    timestamps = pd.to_datetime(df["status_timestamp"], errors="coerce")
    return timestamps <= now


def rule_delivery_id_exists(events_df: pd.DataFrame, valid_delivery_ids: set) -> pd.Series:
    # Vérifie que le delivery_id référencé existe bien dans les livraisons
    return events_df["delivery_id"].isin(valid_delivery_ids)


def rule_driver_id_not_null(df: pd.DataFrame) -> pd.Series:
    return df["driver_id"].notna()


# ---------------------------------------------------------
# Règles sur DRIVERS
# ---------------------------------------------------------

def rule_driver_id_not_null_drivers(df: pd.DataFrame) -> pd.Series:
    return df["driver_id"].notna()


def rule_driver_id_unique(df: pd.DataFrame) -> pd.Series:
    return ~df["driver_id"].duplicated(keep="first")