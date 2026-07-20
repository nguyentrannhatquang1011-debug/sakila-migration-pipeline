# airflow/dags/dag_2_historical_weekly_backfill.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

TABLES_TO_MIGRATE = ['rental', 'payment', 'customer', 'film', 'actor', 'film_actor', 'inventory']

GCS_BUCKET = 'sakila-landing-zone-quang-2026'
BQ_RAW_DATASET = 'sakila_raw'

default_args = {
    'owner': 'senior_mentor',
    'depends_on_past': True,                           # Guarantees chronological week-by-week processing
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_2_historical_weekly_backfill',
    default_args=default_args,
    description='Initial backfill loading Sakila data partitioned physically by last_update column',
    schedule='@weekly',
    start_date=datetime(2005, 1, 1),
    end_date=datetime(2006, 12, 31),
    catchup=True,
    max_active_runs=3,                                 # HARD REQUIREMENT: Concurrency capped at 3
    is_paused_upon_creation=True,                      # Deploys as paused to prevent unintended auto-runs
    tags=['historical_backfill', 'parquet', 'column_partitioned']
) as dag:

    with TaskGroup(group_id='weekly_backfill_ingestion_group') as ingestion_group:
        for table in TABLES_TO_MIGRATE:
            
            gcs_date_partition = "date={{ data_interval_start.strftime('%Y-%m-%d') }}"

            # Task A: Query 7-day historical delta and write Parquet chunks to GCS
            extract_weekly_chunks = MySQLToGCSOperator(
                task_id=f'extract_weekly_{table}_to_gcs',
                mysql_conn_id='mysql_local',
                sql=f"""
                    SELECT * FROM {table} 
                    WHERE last_update >= '{{{{ data_interval_start }}}}' 
                      AND last_update < '{{{{ data_interval_end }}}}'
                """,
                bucket=GCS_BUCKET,
                filename=f'raw/sakila/{table}/{gcs_date_partition}/{table}_{{}}.parquet',
                export_format='parquet',
                approx_max_file_size_bytes=134217728,  # 128 MB Chunk Size
                gcp_conn_id='google_cloud_default'
            )

            # Task B: Append Parquet chunks into BigQuery with Column-Based Partitioning on last_update
            load_weekly_chunks_to_bq = GCSToBigQueryOperator(
                task_id=f'load_weekly_{table}_to_bigquery',
                bucket=GCS_BUCKET,
                source_objects=[f'raw/sakila/{table}/{gcs_date_partition}/*.parquet'],
                destination_project_dataset_table=f'{BQ_RAW_DATASET}.raw_{table}',
                source_format='PARQUET',
                write_disposition='WRITE_APPEND',       # Appends rows; BQ auto-routes to last_update partitions
                autodetect=True,
                # COLUMN-BASED PARTITIONING: Physical partitioning driven by business timestamp
                time_partitioning={
                    "type": "DAY",
                    "field": "last_update"
                },
                gcp_conn_id='google_cloud_default'
            )

            extract_weekly_chunks >> load_weekly_chunks_to_bq

    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id='invoke_dag_3_dbt_transformation',
        trigger_dag_id='dag_3_dbt_transformation',
        wait_for_completion=False,
        reset_dag_run=True,
        allowed_states=['success']
    )

    ingestion_group >> trigger_dbt_transformation

    