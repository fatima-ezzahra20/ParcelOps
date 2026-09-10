"""
parcelops_dag.py

DAG orchestrant le pipeline ParcelOps de bout en bout :
Extract (deliveries, events, drivers) -> Quality Check -> Transform Silver -> Transform Gold
"""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/project")
os.chdir("/opt/airflow/project")  # les scripts écrivent/lisent des chemins relatifs (data/...)

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_extract_deliveries():
    from ingestion.extract_deliveries2 import extract_deliveries
    extract_deliveries()


def run_extract_events():
    from ingestion.extract_events2 import extract_events
    extract_events()


def run_extract_drivers():
    from ingestion.extract_drivers import extract_drivers
    extract_drivers()


def run_quality_checks():
    from quality.checks import check_deliveries, check_drivers, check_events
    valid_deliveries = check_deliveries()
    check_drivers()
    check_events(valid_deliveries)


def run_transform_silver():
    from transformation.to_silver import transform_deliveries, transform_drivers, transform_events
    transform_deliveries()
    transform_drivers()
    transform_events()


def run_transform_gold():
    import os
    import pandas as pd
    from sqlalchemy import create_engine
    from transformation.to_gold2 import (
        upsert_dim_zone, upsert_dim_driver, upsert_dim_date,
        compute_last_status, upsert_fact_deliveries,
    )

    DATABASE_URL = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    engine = create_engine(DATABASE_URL)

    deliveries_df = pd.read_parquet("data/silver/deliveries_silver.parquet")
    drivers_df = pd.read_parquet("data/silver/drivers_silver.parquet")
    events_df = pd.read_parquet("data/silver/events_silver.parquet")

    zone_dim = upsert_dim_zone(deliveries_df, drivers_df, engine)
    drivers_dim = upsert_dim_driver(drivers_df, engine)
    upsert_dim_date(deliveries_df, engine)

    last_status_df = compute_last_status(events_df)
    upsert_fact_deliveries(deliveries_df, drivers_dim, zone_dim, last_status_df, engine)


default_args = {
    "owner": "fatima",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="parcelops_pipeline",
    description="Pipeline ETL ParcelOps : Extract -> Quality -> Silver -> Gold",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=["parcelops"],
) as dag:

    extract_deliveries_task = PythonOperator(
        task_id="extract_deliveries",
        python_callable=run_extract_deliveries,
    )

    extract_events_task = PythonOperator(
        task_id="extract_events",
        python_callable=run_extract_events,
    )

    extract_drivers_task = PythonOperator(
        task_id="extract_drivers",
        python_callable=run_extract_drivers,
    )

    quality_task = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=run_transform_silver,
    )

    gold_task = PythonOperator(
        task_id="transform_gold",
        python_callable=run_transform_gold,
    )

    # Dépendances : les 3 extractions en parallèle -> quality -> silver -> gold
    [extract_deliveries_task, extract_events_task, extract_drivers_task] >> quality_task
    quality_task >> silver_task >> gold_task