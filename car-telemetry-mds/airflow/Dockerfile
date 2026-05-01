FROM apache/airflow:2.9.1

USER root

# System dependencies for psycopg2 and dbt
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
