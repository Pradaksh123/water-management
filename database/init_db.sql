-- init_db.sql
-- Create tables for flow rate and water quality

-- Drop tables if they exist (for reinitialization)
DROP TABLE IF EXISTS flow_rate;
DROP TABLE IF EXISTS water_quality;

-- Table for Flow Rate Data
CREATE TABLE flow_rate (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    location_name VARCHAR(100) NOT NULL CHECK (
        location_name IN (
            'Corporation Water',
            'Ground Water Source 1',
            'Ground Water Source 2',
            'Industrial Process',
            'Tanker Water Supply'
        )
    ),
    totalizer NUMERIC(12, 3) NOT NULL
);

-- Table for Water Quality Data
CREATE TABLE water_quality (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    parameter_name VARCHAR(50) NOT NULL CHECK (
        parameter_name IN (
            'HUMIDITY',
            'ETP (TDS)',
            'ETP (pH)',
            'STP (TDS)',
            'STP (TSS)',
            'STP (BOD)',
            'STP (pH)',
            'STP (COD)'
        )
    ),
    value NUMERIC(10, 3) NOT NULL,
    safe_min NUMERIC(10, 3),
    safe_max NUMERIC(10, 3)
);

-- Insert safe range reference values for parameters
INSERT INTO water_quality (timestamp, parameter_name, value, safe_min, safe_max)
VALUES
    ('2000-01-01 00:00:00', 'HUMIDITY', 0, 30, 70),
    ('2000-01-01 00:00:00', 'ETP (TDS)', 0, 100, 1000),
    ('2000-01-01 00:00:00', 'ETP (pH)', 0, 6.5, 9),
    ('2000-01-01 00:00:00', 'STP (TDS)', 0, 100, 1000),
    ('2000-01-01 00:00:00', 'STP (TSS)', 0, 1000, 3000),
    ('2000-01-01 00:00:00', 'STP (BOD)', 0, 0, 5),
    ('2000-01-01 00:00:00', 'STP (pH)', 0, 6.5, 9),
    ('2000-01-01 00:00:00', 'STP (COD)', 0, 1000, 3000);
