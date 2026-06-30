import json
import logging
import os
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from psycopg2.extras import Json, execute_values


STREAM_FILE_PATH = Path("/opt/airflow/data/car_stream.jsonl")


with DAG(
    dag_id="car_telemetry_pipeline",
    start_date=days_ago(1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["car-telemetry", "modern-data-stack"],
) as dag:
    sense_jsonl_file = FileSensor(
        task_id="sense_jsonl_file",
        filepath="/opt/airflow/data/car_stream.jsonl",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    @task(task_id="load_bronze")
    def load_bronze() -> int:
        if not STREAM_FILE_PATH.exists():
            logging.info("Stream file %s does not exist yet.", STREAM_FILE_PATH)
            return 0

        logging.info("Reading stream file...")

        with STREAM_FILE_PATH.open("r", encoding="utf-8") as stream_file:
            lines = [line.strip() for line in stream_file if line.strip()]

        if not lines:
            logging.info("No telemetry records found.")
            return 0

        logging.info("Loaded %s records.", len(lines))

        records = []

        for line_number, line in enumerate(lines, start=1):
            try:
                payload = json.loads(line)
                records.append((Json(payload),))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON on line {line_number}: {exc.msg}"
                ) from exc

        logging.info("Connecting to PostgreSQL...")

        hook = PostgresHook(postgres_conn_id="postgres_dw")
        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:
                logging.info("Bulk inserting %s records...", len(records))

                execute_values(
                    cursor,
                    """
                    INSERT INTO bronze.raw_car_telemetry (raw_data)
                    VALUES %s
                    """,
                    records,
                    template="(%s)",
                )

            conn.commit()
            logging.info("Insert completed successfully.")

            logging.info("About to truncate stream file...")

            STREAM_FILE_PATH.write_text("", encoding="utf-8")

            logging.info("Stream file truncated.")

            file_size = STREAM_FILE_PATH.stat().st_size
            logging.info("File size after truncate: %s bytes", file_size)

            return len(records)

        except Exception:
            conn.rollback()
            logging.exception("Failed while loading bronze.")
            raise

        finally:
            conn.close()   
    run_dbt = BashOperator(
        task_id="run_dbt",
        cwd="/opt/airflow/dbt_project",
        bash_command="""
        set -euo pipefail
        dbt deps --profiles-dir .
        dbt seed --profiles-dir .
        dbt run --profiles-dir .
        dbt test --profiles-dir .
        """,
        env={
            "DBT_POSTGRES_HOST": os.environ.get("DBT_POSTGRES_HOST", "postgres"),
            "DBT_POSTGRES_PORT": os.environ.get("DBT_POSTGRES_PORT", "5432"),
            "DBT_POSTGRES_USER": os.environ.get("DBT_POSTGRES_USER", "telemetry_user"),
            "DBT_POSTGRES_PASSWORD": os.environ.get("DBT_POSTGRES_PASSWORD", "telemetry_password"),
            "DBT_POSTGRES_DB": os.environ.get("DBT_POSTGRES_DB", "telemetry_dw"),
            "DBT_POSTGRES_SCHEMA": os.environ.get("DBT_POSTGRES_SCHEMA", "public"),
        },
        append_env=True,
    )

    sense_jsonl_file >> load_bronze() >> run_dbt
