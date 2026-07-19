# airflow/dags/dag_2_historical_weekly_backfill.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

# Complete list of operational and master tables to preserve relational integrity
TABLES_TO_MIGRATE = ['rental', 'payment', 'customer', 'film', 'actor', 'film_actor', 'inventory']

GCS_BUCKET = 'sakila-landing-zone-quang-2026'
BQ_RAW_DATASET = 'sakila_raw'

default_args = {
    'owner': 'senior_mentor',
    'depends_on_past': True,                           # Forces strict chronological order during backfills
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_2_historical_weekly_backfill',
    default_args=default_args,
    description='Initial load platform designed to ingest Sakila data in bounded weekly blocks',
    schedule='@weekly',                                # Slices target data timeline into 7-day windows
    start_date=datetime(2005, 1, 1),                   # HARDCODED BOUND: Ingestion window start
    end_date=datetime(2006, 12, 31),                  # HARDCODED BOUND: Ingestion window end
    catchup=True,                                      # Automatically calculates and executes all weeks in range
    max_active_runs=3,                                 # HARD REQUIREMENT: Caps parallel processing at 3 instances
    is_paused_upon_creation=True,                      # Deploys as paused to prevent immediate unintended runs
    tags=['historical_backfill', 'initial_load']
) as dag:

    # TaskGroup to visually bundle parallel extraction-loading workflows
    with TaskGroup(group_id='weekly_backfill_ingestion_group') as ingestion_group:
        for table in TABLES_TO_MIGRATE:
            
            # Structuring GCS directories into clean historical paths
            gcs_partition_path = "backfill/year={{ data_interval_start.strftime('%Y') }}/week={{ data_interval_start.strftime('%U') }}"

            # Task A: Query the weekly delta chunk from MySQL and push JSON profiles to GCS
            extract_weekly_chunks = MySQLToGCSOperator(
                task_id=f'extract_weekly_{table}_to_gcs',
                mysql_conn_id='mysql_local',
                sql=f"""
                    SELECT * FROM {table} 
                    WHERE last_update >= '{{{{ data_interval_start }}}}' 
                      AND last_update < '{{{{ data_interval_end }}}}'
                """,
                bucket=GCS_BUCKET,
                filename=f'raw/sakila/{table}/{gcs_partition_path}/{table}_*.json',
                approx_max_file_size_bytes=10485760,    # Automatically splits files at 10MB boundaries
                export_format='json',
                gcp_conn_id='google_cloud_default'
            )

            # Task B: Append the weekly chunk files directly into your partitioned BigQuery table
            load_weekly_chunks_to_bq = GCSToBigQueryOperator(
                task_id=f'load_weekly_{table}_to_bigquery',
                bucket=GCS_BUCKET,
                source_objects=[f'raw/sakila/{table}/{gcs_partition_path}/*.json'],
                destination_project_dataset_table=f'{BQ_RAW_DATASET}.raw_{table}',
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_APPEND',       # Stacks historical rows cleanly without erasing other weeks
                autodetect=True,
                time_partitioning={"type": "DAY"},     # Enforces native BigQuery daily ingestion-time partitioning
                gcp_conn_id='google_cloud_default'
            )

            extract_weekly_chunks >> load_weekly_chunks_to_bq

    # Task C: Cascade the workflow by triggering your downstream dbt analytical transformations
    # Note: As per instructions, dag_3_dbt_transformation is targeted but not yet implemented
    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id='invoke_dag_3_dbt_transformation',
        trigger_dag_id='dag_3_dbt_transformation',
        wait_for_completion=False,
        reset_dag_run=True,                            # Safely re-runs the target DAG if an identical week run retries
        allowed_states=['success']
    )

    ingestion_group >> trigger_dbt_transformation