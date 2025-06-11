-- init/init.sql
DO $$
BEGIN
    -- Création de l'utilisateur admin
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
        CREATE ROLE admin WITH
        LOGIN
        PASSWORD 'admin'
        NOSUPERUSER
        CREATEDB
        CREATEROLE;
    END IF;
END $$;

-- Création du schéma de base
CREATE SCHEMA IF NOT EXISTS telecom_data;

-- Table des événements simplifiée
CREATE TABLE telecom_data.rated_events (
    event_id SERIAL PRIMARY KEY,
    record_type VARCHAR(10) NOT NULL CHECK (record_type IN ('voice', 'sms', 'data')),
    event_time TIMESTAMP NOT NULL,
    user_identifier VARCHAR(15) NOT NULL,
    cell_id VARCHAR(50) NOT NULL,
    technology VARCHAR(5) NOT NULL,
    cost NUMERIC(10,4) NOT NULL CHECK (cost >= 0),
    duration_sec INT,
    data_volume_mb NUMERIC(10,2)
);

-- Table des factures basique
CREATE TABLE telecom_data.invoices (
    invoice_id SERIAL PRIMARY KEY,
    user_id VARCHAR(15) NOT NULL,
    billing_period_start DATE NOT NULL,
    billing_period_end DATE NOT NULL,
    total_cost NUMERIC(10,4) NOT NULL CHECK (total_cost >= 0),
    paid BOOLEAN NOT NULL DEFAULT FALSE
);

-- Table de liaison simplifiée
CREATE TABLE telecom_data.invoice_line_items (
    invoice_id INT REFERENCES telecom_data.invoices,
    event_id INT REFERENCES telecom_data.rated_events,
    PRIMARY KEY (invoice_id, event_id)
);

-- Permissions de base
GRANT USAGE ON SCHEMA telecom_data TO admin;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA telecom_data TO admin;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA telecom_data TO admin;
