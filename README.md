# LacosteeeTeam-repo
Real-Time Car Telemetry Simulation & Data Pipeline project

> A full end-to-end pipeline that simulates real-world car sensor telemetry, processes it through batch ETL, streams it with real-time alerting, and visualizes it on a live dashboard.

---

## Project Overview

This project builds a pipeline that simulates car sensor data and processes it using both batch and streaming techniques — introducing orchestration, real-time analytics, and cloud-native processing.

---

## Milestones

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Data Simulation & Ingestion | Complete |
| 2 | Batch Data Pipeline (ETL) | Upcoming |
| 3 | Streaming Pipeline with Alerts | Upcoming |
| 4 | Dashboard & Final Report | Upcoming |

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| Simulation | Python 3.10+ |
| Batch ETL | Azure Data Factory / Python (Pandas) *(planned)* |
| Streaming | Azure Stream Analytics / Apache Kafka *(planned)* |
| Storage | Azure Blob Storage / SQL / Data Lake *(planned)* |
| Dashboard | Power BI / Streamlit / Grafana *(planned)* |

---

## Milestone 1 — Data Simulation & Ingestion

**Objective:** Simulate realistic car IoT sensor data and write it to a JSONL stream at a configurable tick rate.

The simulator uses a state-machine driving model with five states (`IDLE`, `ACCELERATING`, `CRUISING`, `BRAKING`, `STOPPED`) and physics-based calculations covering engine RPM, temperatures, gear shifting, fuel consumption, tyre pressure, battery voltage, and ABS activation.

Each tick produces one JSON record appended to `car_stream.jsonl`.

### Getting Started

**Requirements:** Python 3.10+ — no external dependencies.

```bash
python car_simulator.py
```

Console prints a summary every 20 ticks:

```
tick    120 | CRUISING     | spd  74.5 km/h | rpm  2341 | fuel  84.7%
```

Stop with `Ctrl + C` — the file is flushed safely before exit.

**Configuration:**

```python
stream_to_jsonl(
    output_path = "car_stream.jsonl",  # output file
    ticks       = None,                # None = run forever, or set an int
    dt          = 0.5,                 # seconds per tick
    real_time   = True,                # False = run as fast as possible
    flush_every = 1,                   # flush to disk every N ticks
)
```

### Sample Output

```json
{
  "rpm": 2341.6,
  "engine_temp": 91.2,
  "speed_kmh": 74.5,
  "gear": 4,
  "fuel_level": 84.7,
  "fuel_flow": 5.9,
  "battery_voltage": 14.18,
  "tyre_pressure": [2.34, 2.33, 2.29, 2.30],
  "brake_pressure": 0.0,
  "abs_active": false,
  "drive_state": "CRUISING",
  "tick": 120,
  "timestamp": "2026-04-28T17:00:00.000000+00:00"
}
```

---

## Project Structure

```
real-time-iot-pipeline/
│
├── simulation/
│   └── car_simulator.py
│
├── data/
│   └── car_stream.jsonl        # gitignored
│
└── README.md
```

---
*Part of Digital Egypt Pioneers Initiative (depi) graduation project*
