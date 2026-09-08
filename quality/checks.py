"""
checks.py

Orchestre les règles de qualité sur les 3 sources Bronze :
  1. Lit le dernier fichier Bronze de chaque source
  2. Applique les règles définies dans rules.py
  3. Sépare les lignes valides des rejetées (avec la raison du rejet)
  4. Écrit les résultats dans data/quality_reports/
  5. Écrit les lignes valides dans data/silver/ (pré-normalisation,
     la normalisation complète se fera à l'étape 5 - transformation)

Convention : une ligne est REJETÉE si elle échoue à AU MOINS une règle.
"""

import os
import glob
from datetime import datetime

import pandas as pd

from quality import rules


def get_latest_bronze_file(source_prefix: str) -> str:
    """Retourne le fichier Bronze le plus récent pour une source donnée."""
    pattern = f"data/bronze/{source_prefix}_*.parquet"
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"Aucun fichier Bronze trouvé pour '{source_prefix}'. As-tu lancé l'ingestion ?")
    return max(files, key=os.path.getctime)


def write_quality_report(source_name: str, total: int, valid: int, rejected_reasons: dict):
    os.makedirs("data/quality_reports", exist_ok=True)
    report_path = f"data/quality_reports/{source_name}_report.csv"

    rows = [{"rule": "TOTAL", "failed_count": total - valid}]
    for rule_name, count in rejected_reasons.items():
        rows.append({"rule": rule_name, "failed_count": count})

    pd.DataFrame(rows).to_csv(report_path, index=False)

    print(f"\n--- Rapport qualité : {source_name} ---")
    print(f"Total lignes      : {total}")
    print(f"Lignes valides    : {valid} ({valid/total*100:.1f}%)")
    print(f"Lignes rejetées   : {total - valid} ({(total-valid)/total*100:.1f}%)")
    for rule_name, count in rejected_reasons.items():
        if count > 0:
            print(f"  - échec règle '{rule_name}' : {count} lignes")


def check_deliveries():
    path = get_latest_bronze_file("deliveries")
    df = pd.read_parquet(path)
    total = len(df)

    checks = {
        "delivery_id_not_null": rules.rule_delivery_id_not_null(df),
        "delivery_id_unique": rules.rule_delivery_id_unique(df),
        "customer_id_not_null": rules.rule_customer_id_not_null(df),
        "valid_window": rules.rule_valid_window(df),
        "weight_positive": rules.rule_weight_positive(df),
    }

    # Une ligne est valide si elle passe TOUTES les règles
    overall_valid_mask = pd.Series(True, index=df.index)
    rejected_reasons = {}
    for rule_name, mask in checks.items():
        rejected_reasons[rule_name] = (~mask).sum()
        overall_valid_mask &= mask

    valid_df = df[overall_valid_mask]
    rejected_df = df[~overall_valid_mask]

    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/quality_reports", exist_ok=True)
    valid_df.to_parquet("data/silver/deliveries_valid.parquet", index=False)
    rejected_df.to_csv("data/quality_reports/deliveries_rejected.csv", index=False)

    write_quality_report("deliveries", total, len(valid_df), rejected_reasons)
    return valid_df


def check_drivers():
    path = get_latest_bronze_file("drivers")
    df = pd.read_parquet(path)
    total = len(df)

    checks = {
        "driver_id_not_null": rules.rule_driver_id_not_null_drivers(df),
        "driver_id_unique": rules.rule_driver_id_unique(df),
    }

    overall_valid_mask = pd.Series(True, index=df.index)
    rejected_reasons = {}
    for rule_name, mask in checks.items():
        rejected_reasons[rule_name] = (~mask).sum()
        overall_valid_mask &= mask

    valid_df = df[overall_valid_mask]
    rejected_df = df[~overall_valid_mask]

    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/quality_reports", exist_ok=True)
    valid_df.to_parquet("data/silver/drivers_valid.parquet", index=False)
    rejected_df.to_csv("data/quality_reports/drivers_rejected.csv", index=False)

    write_quality_report("drivers", total, len(valid_df), rejected_reasons)
    return valid_df


def check_events(valid_deliveries_df: pd.DataFrame):
    path = get_latest_bronze_file("events")
    df = pd.read_parquet(path)
    total = len(df)

    valid_delivery_ids = set(valid_deliveries_df["delivery_id"])

    checks = {
        "event_id_not_null": rules.rule_event_id_not_null(df),
        "event_not_duplicate": rules.rule_event_not_duplicate(df),
        "valid_status": rules.rule_valid_status(df),
        "timestamp_not_future": rules.rule_timestamp_not_future(df),
        "delivery_id_exists": rules.rule_delivery_id_exists(df, valid_delivery_ids),
        "driver_id_not_null": rules.rule_driver_id_not_null(df),
    }

    overall_valid_mask = pd.Series(True, index=df.index)
    rejected_reasons = {}
    for rule_name, mask in checks.items():
        rejected_reasons[rule_name] = (~mask).sum()
        overall_valid_mask &= mask

    valid_df = df[overall_valid_mask]
    rejected_df = df[~overall_valid_mask]

    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/quality_reports", exist_ok=True)
    valid_df.to_parquet("data/silver/events_valid.parquet", index=False)
    rejected_df.to_csv("data/quality_reports/events_rejected.csv", index=False)

    write_quality_report("events", total, len(valid_df), rejected_reasons)
    return valid_df


if __name__ == "__main__":
    print("Lancement des contrôles de qualité...\n")

    valid_deliveries = check_deliveries()
    valid_drivers = check_drivers()
    # Les events dépendent de deliveries valides pour la règle référentielle
    valid_events = check_events(valid_deliveries)

    print("\nContrôles terminés. Voir data/quality_reports/ pour le détail.")