# Use your pre-existing base image for Airflow 3
FROM astrocrpublic.azurecr.io/runtime:3.3-2

USER root
# Install git (required for cloning dbt packages)
RUN apt-get update && apt-get install -y git && apt-get clean

USER astro

# 1. Install astronomer-cosmos globally so Airflow can render Cosmos DAGs
RUN pip install --no-cache-dir astronomer-cosmos

# 2. Build a virtual environment to isolate dbt dependencies from Airflow 3
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery