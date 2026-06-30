
# 🚗 Car Telemetry Data Engineering Pipeline

## 📖 Overview

This project is an end-to-end Data Engineering pipeline that simulates real-time vehicle telemetry, ingests streaming data into PostgreSQL, transforms it using dbt, and orchestrates the entire workflow with Apache Airflow running inside Docker containers and streams it with real-time alerting, and visualizes it on a live dashboard.

## Milestones

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Data Simulation & Ingestion | Complete |
| 2 | Batch Data Pipeline (ETL) | Complete |
| 3 | Streaming Pipeline with Alerts | Upcoming |
| 4 | Dashboard & Final Report | Upcoming |

The pipeline demonstrates modern Data Engineering concepts including:

* Real-time data simulation
* ETL/ELT pipelines
* Data Warehouse architecture (Bronze → Silver → Gold)
* Workflow orchestration
* Data modeling with dbt
* Data quality testing
* Containerized deployment

---

# 🏗 Architecture

```
Car Simulator
      │
      ▼
car_stream.jsonl
      │
      ▼
Airflow Scheduler
      │
      ▼
Load Bronze
(PostgreSQL)
      │
      ▼
dbt Transformations
      │
      ▼
Silver Layer
      │
      ▼
Gold Layer
      │
      ▼
Analytics Tables
```

---

# ⚙ Technologies Used

* Python
* PostgreSQL
* Apache Airflow
* dbt (Data Build Tool)
* Docker & Docker Compose
* SQL
* JSON
* Bash

---

# 📂 Project Structure

```
project/
│
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── Dockerfile
│
├── simulator/
│   └── car_simulator.py
│
├── dbt_project/
│   ├── models/
│   │
│   ├── staging/
│   ├── intermediate-silver/
│   └── gold/
│       ├── facts/
│       ├── dimensions/
│       └── aggregates/
│
├── postgres/
│   └── init.sql
│
├── docker-compose.yml
│
└── README.md
```

---

# 🚘 Simulated Telemetry

The simulator generates realistic vehicle telemetry including:

* Timestamp
* Speed
* RPM
* Engine Temperature
* Oil Temperature
* Oil Pressure
* Fuel Level
* Fuel Flow
* Battery Voltage
* Brake Pressure
* Odometer
* Trip Distance
* Gear
* Drive State
* Fault Status
* Tire Pressure
* Tire Temperature

Data is continuously written to a shared JSONL file.

---

# 🥉 Bronze Layer

Raw JSON records are ingested directly into PostgreSQL.

Characteristics:

* Raw immutable data
* JSON format preserved
* Audit-friendly
* Source of truth

---

# 🥈 Silver Layer

The Silver layer performs:

* JSON parsing
* Data type conversion
* Data cleaning
* Deduplication
* Derived calculations

### Staging

The staging model materializes as a **table**.

This significantly improves performance by avoiding repeated JSON parsing and expensive window functions.

### Intermediate

Intermediate models remain **views**, allowing reusable business logic without duplicating storage.

---

# 🥇 Gold Layer

The Gold layer contains business-ready analytical models.

### Fact Table

* Vehicle Telemetry Fact

### Dimension Tables

* Driving States
* Gear Groups
* Speed Buckets

### Aggregate Tables

* Driving Behavior
* Vehicle Health
* Subsystem Health
* Trip Summary
* Efficiency Metrics

---

# 🔄 Pipeline Workflow

Every scheduled run executes the following steps:

1. Simulator generates telemetry data
2. Airflow reads the JSONL file
3. Bulk inserts records into PostgreSQL Bronze
4. Stream file is truncated
5. dbt runs transformations
6. dbt executes data quality tests
7. Pipeline finishes successfully

---

# 📊 Data Quality

dbt tests include:

* Unique timestamp + tick validation
* Not Null checks
* Accepted values validation
* Vehicle health validation
* Speed bucket validation

---

# 🚀 Performance Optimization

During development, a major bottleneck was identified.

Originally:

* Staging models were materialized as **views**
* Every downstream model repeatedly parsed millions of JSON records
* PostgreSQL CPU reached 100%
* Queries required several minutes

### Optimization

The staging layer was changed from:

```
View
```

to

```
Table
```

Benefits:

* JSON parsing performed only once
* Reduced repeated window function execution
* Significant reduction in CPU usage
* Stable disk utilization
* dbt execution reduced from several minutes to only a few seconds on development-sized datasets

---

# 🐳 Running the Project

Clone the repository

```
git clone <repository-url>
```

Move into the project

```
cd project
```

Build containers

```
docker compose build
```

Start services

```
docker compose up
```

Open Airflow

```
http://localhost:8080
```

Run the pipeline.

---

# 📈 Future Improvements

* Incremental dbt models
* Partitioned PostgreSQL tables
* Kafka streaming integration
* Azure Data Factory integration
* Azure Synapse Analytics
* Power BI dashboards
* Data lineage visualization
* CI/CD with GitHub Actions

---

# 📚 Learning Outcomes

This project demonstrates practical experience with:

* Data Engineering
* ETL/ELT Pipelines
* Data Warehousing
* Apache Airflow
* dbt
* PostgreSQL
* Docker
* SQL Optimization
* Data Modeling
* Data Quality Testing

---

# 👨‍💻 Author

 - Lacoste Team


