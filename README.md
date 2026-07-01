# 🚗 Car Telemetry Data Engineering Pipeline

## 📖 Overview

This project is an end-to-end **Data Engineering** solution that
combines both **batch** and **real-time streaming** architectures for
vehicle telemetry.

A realistic vehicle simulator continuously generates telemetry data. The
data is processed through two complementary pipelines:

-   **Batch Pipeline:** JSONL → Airflow → PostgreSQL (Bronze) → dbt
    (Silver & Gold)
-   **Streaming Pipeline:** Azure IoT Hub → Azure Stream Analytics →
    Azure SQL Database

The project demonstrates modern cloud data engineering practices
including workflow orchestration, ELT transformations, real-time
ingestion, dimensional modeling, data quality testing, and live
analytics with Power BI.

------------------------------------------------------------------------

# ⭐ Project Highlights

-   End-to-end Batch + Streaming Data Pipeline
-   Apache Airflow orchestration
-   dbt Bronze → Silver → Gold architecture
-   Azure IoT Hub real-time ingestion
-   Azure Stream Analytics SQL transformations
-   Azure SQL live telemetry storage
-   Individual tyre pressure & temperature analytics
-   Dockerized development environment
-   Power BI ready

------------------------------------------------------------------------

## Milestones

  -----------------------------------------------------------------------
  \#        Milestone                           Status
  --------- ----------------------------------- -------------------------
  1         Vehicle Telemetry Simulator         ✅ Complete

  2         Batch ETL Pipeline (Airflow +       ✅ Complete
            PostgreSQL + dbt)                   

  3         Real-Time Streaming Pipeline (Azure ✅ Complete
            IoT Hub → Stream Analytics → Azure  
            SQL)                                

  4         Power BI Dashboard                  🚧 In Progress

  5         Documentation & Portfolio           🚧 In Progress
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 🏗 Architecture

``` text
Vehicle Simulator
        │
 ┌──────┴──────────────┐
 │                     │
 ▼                     ▼
JSONL File        Azure IoT Hub
 │                     │
 ▼                     ▼
Apache Airflow   Azure Stream Analytics
 │                     │
 ▼                     ▼
PostgreSQL Bronze   Azure SQL (telemetry_live)
 │                     │
 ▼                     ▼
dbt Silver/Gold   Live Analytics
        └──────────┬──────────┘
                   ▼
             Power BI Dashboard
```

------------------------------------------------------------------------

# ⚙ Technologies Used

-   Python
-   PostgreSQL
-   Apache Airflow
-   dbt
-   Docker & Docker Compose
-   Azure IoT Hub
-   Azure Stream Analytics
-   Azure SQL Database
-   Power BI
-   SQL
-   JSON

------------------------------------------------------------------------

# 📂 Project Structure

``` text
project/
├── airflow/
├── simulator/
├── dbt_project/
├── postgres/
├── azure/
│   └── stream_analytics_query.sql
│   └── alerts_query.sql
├── docker-compose.yml
└── README.md
```

------------------------------------------------------------------------

# 🚘 Simulated Telemetry

The simulator generates realistic telemetry including:

-   Timestamp
-   Tick
-   Speed
-   RPM
-   Throttle
-   Engine Temperature
-   Oil Temperature
-   Oil Pressure
-   Battery Voltage
-   Fuel Level
-   Fuel Flow
-   Odometer
-   Trip Distance
-   Gear
-   Drive State
-   Ignition
-   Alternator
-   ABS
-   Check Engine
-   Low Fuel Warning
-   Brake Pressure
-   Tyre Pressure FL / FR / RL / RR
-   Tyre Temperature FL / FR / RL / RR
-   Fault Status
-   Fault Type

------------------------------------------------------------------------

# 🔄 Batch Pipeline

Simulator → JSONL → Airflow → PostgreSQL Bronze → dbt Silver → dbt Gold

------------------------------------------------------------------------

# ⚡ Real-Time Streaming Pipeline

Simulator → Azure IoT Hub → Azure Stream Analytics → Azure SQL Database
→ Power BI

------------------------------------------------------------------------

# ☁ Azure SQL Live Schema

The live table stores fields including:

-   timestamp
-   tick
-   speed_kmh
-   rpm
-   throttle
-   brake_pressure
-   tyre_pressure_fl
-   tyre_pressure_fr
-   tyre_pressure_rl
-   tyre_pressure_rr
-   tyre_temp_fl
-   tyre_temp_fr
-   tyre_temp_rl
-   tyre_temp_rr
-   fault_active
-   fault_type

------------------------------------------------------------------------

# 📊 Real-Time Features

-   Live Azure IoT ingestion
-   Stream Analytics SQL processing
-   Azure SQL output
-   Individual tyre analytics
-   Low-latency streaming
-   Power BI DirectQuery ready


------------------------------------------------------------------------

# 📝 Latest Development Update (2026-07-01)

## Simulator Improvements

### Fixed duplicate ingestion issue

A major duplication issue was traced to the Bronze ingestion stage. Records were successfully inserted into PostgreSQL, but the JSONL stream file was not being truncated because of Docker volume permission issues. As a result, Airflow repeatedly reloaded the same telemetry records on subsequent DAG runs.

**Resolution**

- Fixed shared volume write permissions.
- Updated Docker permission handling (`chmod`) for the shared JSONL file.
- Verified that the Bronze task now truncates the stream file successfully after every load.

Verified execution sequence:

```text
Read JSONL
↓
Bulk Insert
↓
Commit
↓
Truncate Stream File
↓
Resume Simulation
```

## Simulator Redesign

The simulator now operates using synchronized batch processing instead of continuously appending data.

New workflow:

```text
Write Batch
↓
Wait for Airflow
↓
JSONL file truncated
↓
Resume Simulation
```

A synchronization loop monitors the JSONL file size before continuing. A 120-second timeout prevents deadlocks if Airflow does not consume the file.

Benefits:

- Prevents duplicate ingestion
- Prevents unbounded file growth
- Keeps simulator synchronized with Airflow

## Pipeline Validation

The complete Batch ETL pipeline was validated after the fixes.

Verified:

- Bronze ingestion
- Silver transformations
- Gold aggregations
- File truncation
- Tick ordering
- Drive state distribution
- Sensor statistics
- Vehicle metrics

## Vehicle Health Review

The current vehicle health thresholds were reviewed and found to be overly aggressive, producing unrealistic distributions of CRITICAL events.

Threshold tuning has been postponed until the streaming alert pipeline is fully operational.

------------------------------------------------------------------------

## Azure Streaming Pipeline

Development continued on the real-time Azure pipeline.

Current architecture:

```text
Vehicle Simulator
        ↓
Azure IoT Hub
        ↓
Azure Stream Analytics
        ↓
Azure SQL Database
        ↓
Power BI
```

### Azure SQL Alert Table

A dedicated `stream_alerts` table was designed to store only active vehicle alerts.

The schema includes:

- Alert metadata
- Vehicle status
- Engine, oil, battery and fuel health
- Tyre summary statistics
- Overall alert severity
- Event timestamps

### Stream Analytics Query

A live alert query was implemented to:

- Process only active faults
- Classify subsystem health
- Calculate overall severity
- Generate live alert records
- Prepare data for Power BI dashboards

### Tyre Analytics Improvements

Instead of storing all four tyre readings in the alert table, the query now summarizes tyre health using:

- Minimum tyre pressure
- Maximum tyre temperature

Future enhancements include:

- Automatic affected tyre detection
- Human-readable failure reasons (Cooling System Failure, Oil Pump Failure, Tyre Puncture, etc.)

------------------------------------------------------------------------

## Stream Analytics Debugging

Extensive debugging was performed throughout the Azure streaming pipeline.

Verified successfully:

- Azure IoT Hub receives telemetry
- Stream Analytics receives events
- Test queries execute successfully
- No runtime errors
- No deserialization errors
- No data conversion errors

-------------------------------------------------------------------------

## Current Project Status

### Completed

- Vehicle telemetry simulator
- Batch ETL pipeline
- Bronze duplicate ingestion fix
- Airflow file synchronization
- dbt Silver & Gold models
- Azure IoT Hub integration
- Azure Stream Analytics pipeline framework
- Azure SQL alert schema


### Planned

- Build final Power BI dashboards
- Complete deployment documentation


------------------------------------------------------------------------

# 📚 Learning Outcomes

-   Data Engineering
-   Batch Processing
-   Streaming Data Engineering
-   Apache Airflow
-   dbt
-   PostgreSQL
-   Azure IoT Hub
-   Azure Stream Analytics
-   Azure SQL Database
-   Docker
-   Data Warehousing
-   SQL Optimization

------------------------------------------------------------------------

# 👨‍💻 Author

**Lacoste Team**
