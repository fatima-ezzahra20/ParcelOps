-- Schéma de la base SOURCE (données opérationnelles brutes)
-- Différent du futur schéma Gold (Star Schema) qu'on créera plus tard

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    pickup_date DATE NOT NULL,
    delivery_zone VARCHAR(50),
    scheduled_window_start TIMESTAMP,
    scheduled_window_end TIMESTAMP,
    package_weight_kg FLOAT,
    priority VARCHAR(20),
    updated_at TIMESTAMP NOT NULL
);