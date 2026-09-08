-- Schéma GOLD : Star Schema pour l'analyse des livraisons
-- fact_deliveries est la table de faits, dim_* sont les dimensions

DROP TABLE IF EXISTS fact_deliveries;
DROP TABLE IF EXISTS dim_zone;
DROP TABLE IF EXISTS dim_driver;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_zone (
    zone_key SERIAL PRIMARY KEY,
    zone_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE dim_driver (
    driver_key SERIAL PRIMARY KEY,
    driver_id VARCHAR(20) UNIQUE NOT NULL,
    driver_name VARCHAR(100),
    vehicle_type VARCHAR(20),
    contract_type VARCHAR(20),
    active BOOLEAN
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,        -- format YYYYMMDD
    full_date DATE NOT NULL,
    day INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    year INT,
    day_of_week VARCHAR(20),
    is_weekend BOOLEAN
);

CREATE TABLE fact_deliveries (
    delivery_key SERIAL PRIMARY KEY,
    delivery_id VARCHAR(20) UNIQUE NOT NULL,
    zone_key INT REFERENCES dim_zone(zone_key),
    driver_key INT REFERENCES dim_driver(driver_key),
    date_key INT REFERENCES dim_date(date_key),

    package_weight_kg FLOAT,
    priority VARCHAR(20),
    scheduled_window_start TIMESTAMP,
    scheduled_window_end TIMESTAMP,

    last_status VARCHAR(20),
    last_status_timestamp TIMESTAMP,

    is_late_now BOOLEAN,
    was_delivered_late BOOLEAN,

    loaded_at TIMESTAMP DEFAULT NOW()
);