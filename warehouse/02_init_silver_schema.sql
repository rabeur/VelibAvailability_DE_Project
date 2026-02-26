-- ============================================================================
-- SILVER LAYER SCHEMA - Vélib Data Engineering Project
-- ============================================================================
-- Description: Schéma pour les données nettoyées et normalisées
-- Layer: Silver (Clean & Normalized)
-- Author: Data Team
-- Date: 2026-02-26
-- ============================================================================

-- Créer le schéma silver
CREATE SCHEMA IF NOT EXISTS silver;

-- ============================================================================
-- TABLE: silver.stations (Dimension - SCD Type 2)
-- ============================================================================
-- Description: Dimension des stations Vélib avec historique des changements
-- ============================================================================

DROP TABLE IF EXISTS silver.station_availability CASCADE;
DROP TABLE IF EXISTS silver.stations CASCADE;

CREATE TABLE silver.stations (
    -- Identifiants
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(255) NOT NULL,

    -- Caractéristiques physiques
    capacity INTEGER NOT NULL CHECK (capacity >= 0),

    -- Localisation
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    district_municipality_names VARCHAR(255),
    insee_municipality_code VARCHAR(10),

    -- Métadonnées temporelles
    is_active BOOLEAN DEFAULT TRUE,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour améliorer les performances
CREATE INDEX idx_stations_geo ON silver.stations(latitude, longitude);
CREATE INDEX idx_stations_arrondissement ON silver.stations(district_municipality_names);
CREATE INDEX idx_stations_active ON silver.stations(is_active);
CREATE INDEX idx_stations_last_seen ON silver.stations(last_seen_at);

-- Commentaires
COMMENT ON TABLE silver.stations IS 'Velib stations dimension with static information';
COMMENT ON COLUMN silver.stations.station_id IS 'Station unique identifier (stationcode)';
COMMENT ON COLUMN silver.stations.capacity IS 'Station total capacity';
COMMENT ON COLUMN silver.stations.is_active IS 'Indicator if the station is active (seen recently)';

-- ============================================================================
-- TABLE: silver.station_availability (Fait)
-- ============================================================================
-- Description: Faits de disponibilité des stations par snapshot
-- ============================================================================

CREATE TABLE silver.station_availability (
    -- Clé primaire auto-incrémentée
    id BIGSERIAL PRIMARY KEY,

    -- Clés étrangères
    station_id VARCHAR(50) NOT NULL REFERENCES silver.stations(station_id),

    -- Dimension temporelle
    snapshot_timestamp TIMESTAMP NOT NULL,
    snapshot_date DATE NOT NULL GENERATED ALWAYS AS (snapshot_timestamp::DATE) STORED,
    snapshot_hour INTEGER NOT NULL GENERATED ALWAYS AS (EXTRACT(HOUR FROM snapshot_timestamp)) STORED,
    snapshot_day_of_week INTEGER NOT NULL GENERATED ALWAYS AS (EXTRACT(DOW FROM snapshot_timestamp)) STORED,

    -- Métriques de disponibilité - Vélos
    num_bikes_available INTEGER NOT NULL CHECK (num_bikes_available >= 0),
    num_bikes_available_mechanical INTEGER CHECK (num_bikes_available_mechanical >= 0),
    num_bikes_available_ebike INTEGER CHECK (num_bikes_available_ebike >= 0),

    -- Métriques de disponibilité - Places
    num_docks_available INTEGER NOT NULL CHECK (num_docks_available >= 0),

    -- Status opérationnel
    is_installed BOOLEAN NOT NULL DEFAULT TRUE,
    is_renting BOOLEAN NOT NULL DEFAULT TRUE,
    is_returning BOOLEAN NOT NULL DEFAULT TRUE,

    -- Métriques calculées
    occupancy_rate DECIMAL(5, 2) CHECK (occupancy_rate BETWEEN 0 AND 100),
    availability_rate DECIMAL(5, 2) CHECK (availability_rate BETWEEN 0 AND 100),
    service_rate DECIMAL(5, 2) CHECK (service_rate BETWEEN 0 AND 100),
    is_empty BOOLEAN NOT NULL DEFAULT FALSE,
    is_full BOOLEAN NOT NULL DEFAULT FALSE,
    is_operational BOOLEAN NOT NULL DEFAULT TRUE,

    -- Métadonnées d'ingestion
    ingestion_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Contrainte d'unicité : une seule mesure par station et timestamp
    CONSTRAINT uk_station_snapshot UNIQUE(station_id, snapshot_timestamp)
);

-- Index pour optimiser les requêtes analytiques
CREATE INDEX idx_availability_timestamp ON silver.station_availability(snapshot_timestamp);
CREATE INDEX idx_availability_date ON silver.station_availability(snapshot_date);
CREATE INDEX idx_availability_station_time ON silver.station_availability(station_id, snapshot_timestamp DESC);
CREATE INDEX idx_availability_hour ON silver.station_availability(snapshot_hour);
CREATE INDEX idx_availability_dow ON silver.station_availability(snapshot_day_of_week);
CREATE INDEX idx_availability_occupancy ON silver.station_availability(occupancy_rate);
CREATE INDEX idx_availability_empty ON silver.station_availability(is_empty) WHERE is_empty = TRUE;
CREATE INDEX idx_availability_full ON silver.station_availability(is_full) WHERE is_full = TRUE;
CREATE INDEX idx_availability_operational ON silver.station_availability(is_operational);

-- Partitionnement par date (optionnel, à décommenter si volumes importants)
-- CREATE TABLE silver.station_availability_2026_02 PARTITION OF silver.station_availability
-- FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- Commentaires
COMMENT ON TABLE silver.station_availability IS 'Station availability facts table by timestamp snapshot';
COMMENT ON COLUMN silver.station_availability.occupancy_rate IS 'Occupation rate = (bikes_available / capacity) * 100';
COMMENT ON COLUMN silver.station_availability.availability_rate IS 'Avaibility rate = (docks_available / capacity) * 100';
COMMENT ON COLUMN silver.station_availability.service_rate IS 'Service rate = (bikes_available + docks_available / capacity) * 100';
COMMENT ON COLUMN silver.station_availability.is_empty IS 'Station empty (0 bikes available)';
COMMENT ON COLUMN silver.station_availability.is_full IS 'Station full (0 docks available)';
COMMENT ON COLUMN silver.station_availability.is_operational IS 'Operational station (is_installed AND is_renting AND is_returning)';

-- ============================================================================
-- VUES UTILITAIRES
-- ============================================================================

-- Vue : Dernière disponibilité par station
CREATE OR REPLACE VIEW silver.v_latest_station_availability AS
SELECT DISTINCT ON (sa.station_id)
    s.station_id,
    s.station_name,
    s.district_municipality_names,
    s.capacity,
    sa.snapshot_timestamp,
    sa.num_bikes_available,
    sa.num_bikes_available_mechanical,
    sa.num_bikes_available_ebike,
    sa.num_docks_available,
    sa.occupancy_rate,
    sa.availability_rate,
    sa.service_rate,
    sa.is_empty,
    sa.is_full,
    sa.is_operational,
    s.latitude,
    s.longitude,
    s.insee_municipality_code
FROM silver.station_availability sa
JOIN silver.stations s ON sa.station_id = s.station_id
ORDER BY sa.station_id, sa.snapshot_timestamp DESC;

COMMENT ON VIEW silver.v_latest_station_availability IS 'View of the latest known availability for each station';

-- Vue : Statistiques quotidiennes par station
CREATE OR REPLACE VIEW silver.v_daily_station_stats AS
SELECT
    station_id,
    snapshot_date,
    COUNT(*) as num_snapshots,
    ROUND(AVG(num_bikes_available), 2) as avg_bikes_available,
    ROUND(AVG(num_docks_available), 2) as avg_docks_available,
    ROUND(AVG(occupancy_rate), 2) as avg_occupancy_rate,
    ROUND(AVG(service_rate), 2) as avg_service_rate,
    MAX(num_bikes_available) as max_bikes_available,
    MIN(num_bikes_available) as min_bikes_available,
    SUM(CASE WHEN is_empty THEN 1 ELSE 0 END) as times_empty,
    SUM(CASE WHEN is_full THEN 1 ELSE 0 END) as times_full,
    ROUND(AVG(CASE WHEN is_operational THEN 1 ELSE 0 END) * 100, 2) as operational_pct
FROM silver.station_availability
GROUP BY station_id, snapshot_date;

COMMENT ON VIEW silver.v_daily_station_stats IS 'Daily aggregated statistics by station';
COMMENT ON COLUMN silver.v_daily_station_stats.times_empty IS 'Number of times (in minutes) a station was empty (0 bikes available)';
COMMENT ON COLUMN silver.v_daily_station_stats.times_full IS 'Number of times (in minutes) a station was full (0 docks available)';


-- ============================================================================
-- FONCTIONS UTILITAIRES
-- ============================================================================

-- Fonction : Mettre à jour le timestamp de dernière modification
CREATE OR REPLACE FUNCTION silver.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger : Auto-update du updated_at sur la table stations
CREATE TRIGGER trigger_stations_updated_at
    BEFORE UPDATE ON silver.stations
    FOR EACH ROW
    EXECUTE FUNCTION silver.update_updated_at_column();

-- ============================================================================
-- GRANTS (à adapter selon vos utilisateurs)
-- ============================================================================

-- GRANT USAGE ON SCHEMA silver TO airflow_user;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA silver TO airflow_user;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA silver TO airflow_user;

-- ============================================================================
-- STATISTIQUES & MAINTENANCE
-- ============================================================================

-- Analyser les tables pour l'optimiseur de requêtes
ANALYZE silver.stations;
ANALYZE silver.station_availability;

-- Afficher un résumé
SELECT
    'silver.stations' as table_name,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('silver.stations')) as total_size
FROM silver.stations
UNION ALL
SELECT
    'silver.station_availability' as table_name,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('silver.station_availability')) as total_size
FROM silver.station_availability;

-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================