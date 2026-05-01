# 🏎️ Car Telemetry — Modern Data Stack

> A production-ready, containerized data pipeline for car sensor telemetry.
> Simulates real-time IoT data and processes it through a full medallion
> architecture (Bronze → Silver → Gold) using Airflow, dbt, and PostgreSQL.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Compose                               │
│                                                                     │
│  ┌──────────────┐    shared_data     ┌───────────────────────────┐  │
│  │  Simulator    │───── volume ──────▶│  Airflow                  │  │
│  │  (Python 3.11)│   /data ↔          │  ┌─────────┐ ┌─────────┐ │  │
│  │               │   /opt/airflow/    │  │Webserver│ │Scheduler│ │  │
│  │  car_stream   │   data             │  │ :8080   │ │         │ │  │
│  │  .jsonl       │                    │  └─────────┘ └─────────┘ │  │
│  └──────────────┘                    │       │                   │  │
│   simulator-net                       │       │ DAG Pipeline      │  │
│                                       │       ▼                   │  │
│                                       │  1. FileSensor            │  │
│                                       │  2. load_bronze (Python)  │  │
│                                       │  3. run_dbt (Bash)        │  │
│                                       └───────────┬───────────────┘  │
│                                                   │ backend net      │
│                                                   ▼                  │
│                                       ┌───────────────────────┐      │
│                                       │  PostgreSQL 15        │      │
│                                       │  ┌───────┐            │      │
│                                       │  │bronze │ raw JSONB  │      │
│                                       │  ├───────┤            │      │
│                                       │  │silver │ dbt views  │      │
│                                       │  ├───────┤            │      │
│                                       │  │gold   │ dbt tables │      │
│                                       │  └───────┘            │      │
│                                       │        :5432          │      │
│                                       └───────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Simulator → JSONL file → Airflow FileSensor → Bronze (raw JSONB)
                                                  │
                                           dbt run │
                                                  ▼
                                    Silver (staging views)
                                    ├── stg_car_telemetry
                                    └── int_telemetry_unpacked
                                                  │
                                           dbt run │
                                                  ▼
                                     Gold (materialized tables)
                                     ├── fct_telemetry (incremental)
                                     ├── fct_anomalies
                                     ├── dim_driving_states
                                     └── agg_trip_summary (incremental)
```

---

## Prerequisites

| Tool              | Version   |
|-------------------|-----------|
| Docker            | 24+       |
| Docker Compose    | v2        |

---

## Quick Start

### 1. Start all services

```bash
cd car-telemetry-mds
docker compose up -d --build
```

### 2. Verify services are healthy

```bash
docker compose ps
```

Wait until all services show `healthy` / `running` status.

### 3. Watch the simulator

```bash
docker compose logs -f simulator
```

You should see telemetry ticks streaming every 0.5 seconds:

```
tick    120 | CRUISING     | spd  74.5 km/h | rpm  2341 | fuel  84.7%
```

### 4. Access Airflow UI

Open [http://localhost:8080](http://localhost:8080) in your browser.

| Field    | Value   |
|----------|---------|
| Username | `admin` |
| Password | `admin` |

The DAG `car_telemetry_pipeline` runs every 5 minutes automatically,
or you can trigger it manually from the UI.

---

## Running dbt Manually

To run dbt commands inside the Airflow container:

```bash
# Open a shell in the webserver container
docker compose exec airflow-webserver bash

# Navigate to the dbt project
cd /opt/airflow/dbt_project

# Install packages
dbt deps --profiles-dir .

# Run all models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Generate docs
dbt docs generate --profiles-dir .
```

---

## Connecting to PostgreSQL

```bash
# From your host machine
psql -h localhost -p 5435 -U telemetry -d car_telemetry_dw
# Password: telemetry_secret_2024

# Example queries
SELECT count(*) FROM bronze.raw_car_telemetry;
SELECT * FROM silver.stg_car_telemetry LIMIT 5;
SELECT * FROM gold.fct_telemetry LIMIT 5;
SELECT * FROM gold.fct_anomalies LIMIT 10;
SELECT * FROM gold.agg_trip_summary;
SELECT * FROM gold.dim_driving_states;
```

---

## DAG Pipeline Details

| Task              | Type           | Description                                  |
|-------------------|----------------|----------------------------------------------|
| `sense_jsonl_file`| FileSensor     | Waits for `car_stream.jsonl` to appear       |
| `load_bronze`     | PythonOperator | Bulk-inserts JSONL → `bronze.raw_car_telemetry`, truncates file |
| `run_dbt`         | BashOperator   | Runs `dbt deps && dbt run && dbt test`       |

---

## dbt Models

| Layer         | Model                    | Materialization | Description                              |
|---------------|--------------------------|-----------------|------------------------------------------|
| **Staging**   | `stg_car_telemetry`      | view            | Extracts & deduplicates JSONB fields     |
| **Intermediate** | `int_telemetry_unpacked` | view         | Adds °F conversions, fuel calc, speed bucket |
| **Marts**     | `fct_telemetry`          | incremental     | Full enriched telemetry fact table       |
| **Marts**     | `fct_anomalies`          | table           | Anomalous ticks with flag arrays         |
| **Marts**     | `dim_driving_states`     | table           | Static dimension from seed               |
| **Marts**     | `agg_trip_summary`       | incremental     | Aggregated metrics by state × speed      |

---

## Resetting the Pipeline

To completely reset and start fresh:

```bash
# Stop all containers and remove volumes
docker compose down -v

# Rebuild and start
docker compose up -d --build
```

This destroys all data (PostgreSQL, shared JSONL files) and starts from scratch.

---

## Project Structure

```
car-telemetry-mds/
├── docker-compose.yml          # Service orchestration
├── .env                        # Secrets & environment variables
├── simulator/
│   ├── car_simulator.py        # Physics-based car sensor simulator
│   └── Dockerfile              # Python 3.11-slim image
├── airflow/
│   ├── Dockerfile              # Airflow 2.9.1 + dbt + psycopg2
│   ├── requirements.txt        # Python dependencies
│   └── dags/
│       └── car_telemetry_pipeline.py  # Orchestration DAG
├── dbt_project/
│   ├── dbt_project.yml         # dbt project configuration
│   ├── profiles.yml            # Connection profiles (env-var driven)
│   ├── packages.yml            # dbt_utils dependency
│   ├── macros/
│   │   └── temperature_utils.sql      # Celsius→°F & fuel macros
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_car_telemetry.sql  # JSONB extraction + dedup
│   │   │   └── schema.yml            # Source & test definitions
│   │   ├── intermediate/
│   │   │   └── int_telemetry_unpacked.sql  # Derived columns
│   │   └── marts/
│   │       ├── fct_telemetry.sql      # Incremental fact table
│   │       ├── fct_anomalies.sql      # Anomaly detection
│   │       ├── dim_driving_states.sql # Static dimension
│   │       ├── agg_trip_summary.sql   # Trip aggregations
│   │       └── schema.yml            # Mart test definitions
│   └── seeds/
│       └── drive_state_labels.csv     # Dimension seed data
├── postgres/
│   └── init.sql                # Bronze schema DDL
└── README.md                   # This file
```

---

*Part of Digital Egypt Pioneers Initiative (DEPI) graduation project — LacosteeeTeam*
