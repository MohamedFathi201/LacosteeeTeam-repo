FROM apache/airflow:2.9.1-python3.11

USER airflow

ARG AIRFLOW_VERSION=2.9.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir --constraint "${CONSTRAINTS_URL}" -r /opt/airflow/requirements.txt
