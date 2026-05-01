"""
Car Telemetry Pipeline DAG
==========================
Ingests JSONL sensor data from the simulator into the bronze layer,
then runs dbt to transform through silver → gold.

Schedule: every 5 minutes
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

JSONL_PATH = "/opt/airflow/data/car_stream.jsonl"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="car_telemetry_pipeline",
    description="Ingest car telemetry JSONL → bronze, then dbt silver/gold",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["telemetry", "car", "dbt"],
)
def car_telemetry_pipeline():
    # ── Task 1: Wait for the JSONL file to appear ────────────
    sense_jsonl_file = FileSensor(
        task_id="sense_jsonl_file",
        filepath=JSONL_PATH,
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    # ── Task 2: Load raw JSON lines into bronze ──────────────
    @task(task_id="load_bronze")
    def load_bronze() -> int:
        """Read all JSONL lines, bulk-insert into bronze.raw_car_telemetry,
        then truncate the file so the next run starts fresh."""

        jsonl_file = Path(JSONL_PATH)

        if not jsonl_file.exists():
            logger.info("JSONL file does not exist yet — waiting for simulator.")
            return 0

        # Read all lines
        lines = jsonl_file.read_text(encoding="utf-8").strip().splitlines()
        if not lines:
            logger.info("JSONL file is empty — nothing to ingest.")
            return 0

        # Parse each line as JSON
        records = []
        for line in lines:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning("Skipping malformed line: %s — %s", line[:80], e)

        if not records:
            logger.info("No valid JSON records found.")
            return 0

        # Bulk-insert via psycopg2
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        conn = hook.get_conn()
        cur = conn.cursor()

        insert_sql = (
            "INSERT INTO bronze.raw_car_telemetry (raw_data) VALUES (%s)"
        )
        params = [(json.dumps(r),) for r in records]

        cur.executemany(insert_sql, params)
        conn.commit()
        cur.close()
        conn.close()

        row_count = len(records)
        logger.info("Inserted %d rows into bronze.raw_car_telemetry", row_count)

        # Truncate the file so the next run doesn't re-ingest
        with open(JSONL_PATH, "w", encoding="utf-8") as f:
            pass  # empty the file

        logger.info("Truncated %s after successful load.", JSONL_PATH)
        return row_count

    # ── Task 3: Run dbt (deps → run → test) ─────────────────
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt deps --profiles-dir . && "
            "dbt run --profiles-dir . && "
            "dbt test --profiles-dir ."
        ),
        env={
            "DBT_POSTGRES_HOST": os.environ.get("DBT_POSTGRES_HOST", "postgres"),
            "DBT_POSTGRES_USER": os.environ.get("DBT_POSTGRES_USER", "telemetry"),
            "DBT_POSTGRES_PASSWORD": os.environ.get("DBT_POSTGRES_PASSWORD", ""),
            "DBT_POSTGRES_DB": os.environ.get("DBT_POSTGRES_DB", "car_telemetry_dw"),
            "DBT_POSTGRES_PORT": os.environ.get("DBT_POSTGRES_PORT", "5432"),
            "DBT_POSTGRES_SCHEMA": os.environ.get("DBT_POSTGRES_SCHEMA", "public"),
        },
        append_env=True,
    )

    # ── Wire dependencies ────────────────────────────────────
    bronze_task = load_bronze()
    sense_jsonl_file >> bronze_task >> run_dbt


# Instantiate the DAG
car_telemetry_pipeline()
