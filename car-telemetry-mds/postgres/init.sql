-- ============================================================
-- PostgreSQL init script for Car Telemetry Data Warehouse
-- Creates bronze / silver / gold medallion schemas
-- ============================================================

-- ─── Schemas ─────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ─── Bronze Layer ────────────────────────────────────────────
-- Raw JSONL records ingested from the simulator stream file.
-- Each row stores one JSON tick as-is inside a JSONB column.

CREATE TABLE IF NOT EXISTS bronze.raw_car_telemetry (
    id           BIGSERIAL       PRIMARY KEY,
    ingested_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    raw_data     JSONB           NOT NULL
);

-- GIN index for fast JSONB containment / key-exists queries
CREATE INDEX IF NOT EXISTS idx_raw_car_telemetry_gin
    ON bronze.raw_car_telemetry USING GIN (raw_data);

-- Silver and gold schemas are managed by dbt — no tables created here.
