# =========================================================================
# STAGE 1: Isolated dbt Environment Builder (Cached & Immune to DAG changes)
# =========================================================================
FROM python:3.11-slim AS dbt-environment-builder

USER root
# Install git for dbt package tracking
RUN apt-get update && apt-get install -y git && apt-get clean

# Construct and compile the isolated dbt virtual environment
RUN python -m venv /dbt_venv && \
    /dbt_venv/bin/pip install --no-cache-dir dbt-bigquery

# =========================================================================
# STAGE 2: Core Airflow Runtime (Leveraging quick file copies)
# =========================================================================
FROM astrocrpublic.azurecr.io/runtime:3.3-2

USER root
# Install runtime git dependencies if required by core Airflow tasks
RUN apt-get update && apt-get install -y git && apt-get clean

USER astro

# Atomic copy operation of the pre-built virtual environment from Stage 1
# This takes less than a second even when DAG files are modified!
COPY --from=dbt-environment-builder /dbt_venv /usr/local/airflow/dbt_venv