CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.raw_car_telemetry (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_data JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_car_telemetry_raw_data_gin
    ON bronze.raw_car_telemetry
    USING GIN (raw_data);
