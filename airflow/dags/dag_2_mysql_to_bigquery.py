# airflow/dags/dag_2_mysql_to_bigquery.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

# Tables to migrate
TABLES_TO_MIGRATE = ['rental', 'payment', 'customer', 'film', 'actor', 'film_actor', 'inventory']

GCS_BUCKET = 'sakila-landing-zone-quang-2026'
BQ_RAW_DATASET = 'sakila_raw'

default_args = {
    'owner': 'senior_mentor',
    'depends_on_past': True,                           # Ensures historical sequences run in order
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_2_mysql_to_bigquery',
    default_args=default_args,
    description='Incremental partition-level migration of Sakila from MySQL to GCS and BQ',
    schedule='@daily',                                 # Scheduled daily to match the generator
    start_date=datetime(2026, 7, 1),
    max_active_runs=3,                                
    catchup=True,                                      # True allows automatic incremental backfilling!
    tags=['production_ingestion', 'incremental']
) as dag:

    with TaskGroup(group_id='incremental_ingestion_group') as ingestion_group:
        for table in TABLES_TO_MIGRATE:
            
            # Helper to format our daily partitioning variables
            # Explain "{{ data_interval_start.strftime('%Y') }}": 
            # This is a Jinja template expression that formats the start of the data interval (the beginning of the day for which the DAG is running) into a string representing the year. 
            # The same applies to month and day formatting. These expressions are used to dynamically create partition paths and partition dates based on the execution date of the DAG.
            partition_path = "year={{ data_interval_start.strftime('%Y') }}/month={{ data_interval_start.strftime('%m') }}/day={{ data_interval_start.strftime('%d') }}"
            partition_date = "{{ data_interval_start.strftime('%Y%m%d') }}"

            # Task A: Incremental Extract & Chunk to GCS
            extract_to_gcs = MySQLToGCSOperator(
                task_id=f'extract_{table}_to_gcs',
                mysql_conn_id='mysql_local',
                # 1. TEMPORAL WATERMARK: Filters source data to only grab the daily delta
                # data_interval_start: 2026-07-01 00:00:00
                # data_interval_end: 2026-07-02 00:00:00
                sql=f"""
                    SELECT * FROM {table} 
                    WHERE last_update >= '{{{{ data_interval_start }}}}' 
                      AND last_update < '{{{{ data_interval_end }}}}'
                """,
                bucket=GCS_BUCKET,
                # 2. HIVE PARTITION PATHS: Writes to date-specific subdirectories
                filename=f'raw/sakila/{table}/{partition_path}/{table}_*.json',
                # Explain "{table}_*.json": The asterisk (*) is a wildcard that allows for multiple files to be created if the data exceeds the specified chunk size. 
                # Each file will have a unique name starting with the table name followed by an underscore and a unique identifier.

                # 3. FILE CHUNKING: Automatically splits files if they exceed 10MB (10,485,760 bytes)
                approx_max_file_size_bytes=10485760,
                export_format='json',
                # FIXED: Removed allow_empty=True. 
                # write_on_empty defaults to False, meaning 0 rows will exit cleanly with success.                      # Prevents task failure if a table had 0 updates today
                gcp_conn_id='google_cloud_default'
            )

            # Task B: Idempotent Load to Partitioned BigQuery Staging Table
            load_to_bq = GCSToBigQueryOperator(
                task_id=f'load_{table}_to_bigquery',
                bucket=GCS_BUCKET,
                # Read all chunked JSON files inside the daily path using GCS Wildcards (*)
                source_objects=[f'raw/sakila/{table}/{partition_path}/*.json'],
                # 4. PARTITION DECORATOR ($YYYYMMDD): Overwrites ONLY today's specific partition slice
                destination_project_dataset_table=f'{BQ_RAW_DATASET}.raw_{table}${partition_date}',
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_TRUNCATE',    # Safely overwrites today's partition on retry
                autodetect=True,
                # Configures the BQ table to be partitioned by ingestion time automatically
                time_partitioning={"type": "DAY"},
                gcp_conn_id='google_cloud_default'
            )

            extract_to_gcs >> load_to_bq

    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id='invoke_dag_3_dbt',
        trigger_dag_id='dag_3_dbt_transformation',
        wait_for_completion=False,
        reset_dag_run=True,
        allowed_states=['success']
    )

    ingestion_group >> trigger_dbt_transformation