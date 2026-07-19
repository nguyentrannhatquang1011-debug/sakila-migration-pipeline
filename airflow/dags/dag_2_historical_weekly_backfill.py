# airflow/dags/dag_2_historical_weekly_backfill.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

TABLES_TO_MIGRATE = ['rental', 'payment', 'customer', 'film', 'actor', 'film_actor', 'inventory']
GCS_BUCKET = 'sakila-landing-zone-quang-2026'
BQ_RAW_DATASET = 'sakila_raw'

default_args = {
    'owner': 'senior_mentor',
    'depends_on_past': False,                          # False allows manual isolated runs
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='dag_2_historical_weekly_backfill',
    default_args=default_args,
    description='Initial load to backfill Sakila data week-by-week via UI parameters',
    schedule=None,                                     # Manual/Trigger-only execution model
    start_date=datetime(2026, 1, 1),
    max_active_runs=3,                                 # HARD REQUIREMENT: Caps cluster instance concurrency
    catchup=False,
    tags=['historical_backfill', 'ui_parameterized'],
    # Defines the interactive input form inside the Airflow web dashboard
    params={
        "start_date": Param(default="2025-01-01", type="string", format="date", description="Ngày bắt đầu nạp dữ liệu (YYYY-MM-DD)"),
        "end_date": Param(default="2025-03-01", type="string", format="date", description="Ngày kết thúc nạp dữ liệu (YYYY-MM-DD)"),
        # Explain "current_pointer": This parameter is used to track the current position in the historical backfill process. 
        # It allows the DAG to know where to start the next chunk of data extraction. 
        # When the DAG is triggered for the first time, this parameter should be left blank, and it will be set automatically based on the start_date parameter. 
        # For subsequent runs, it will be updated to point to the next week of data to be processed.
        "current_pointer": Param(default="", type=["string", "null"], description="System tracking field. Leave blank on initial trigger.")
    }
) as dag:

    # Task 1: Coordinate window state and compute temporal delta bounds
    def compute_window_bounds(**context):
        params = context['params']
        
        # Hydrate active pointer tracking location
        active_start_str = params.get('current_pointer') or params.get('start_date')
        global_end_str = params.get('end_date')
        
        active_start_dt = datetime.strptime(active_start_str, "%Y-%m-%d")
        global_end_dt = datetime.strptime(global_end_str, "%Y-%m-%d")
        
        # Step out exactly 1 week
        calculated_end_dt = active_start_dt + timedelta(days=7)
        
        # Enforce hard ceiling constraints
        if calculated_end_dt >= global_end_dt:
            calculated_end_dt = global_end_dt
            loop_complete = True
        else:
            loop_complete = False
            
        return {
            "window_start": active_start_dt.strftime("%Y-%m-%d"),
            "window_end": calculated_end_dt.strftime("%Y-%m-%d"),
            "window_nodash": active_start_dt.strftime("%Y%m%d"),
            "next_pointer": calculated_end_dt.strftime("%Y-%m-%d"),
            "loop_complete": loop_complete
        }

    parse_dates = PythonOperator(
        task_id='parse_dates',
        python_callable=compute_window_bounds
    )

    # TaskGroup: Handles chunked data replication for this iteration
    with TaskGroup(group_id='weekly_chunk_migration') as ingestion_group:
        for table in TABLES_TO_MIGRATE:
            
            extract_to_gcs = MySQLToGCSOperator(
                task_id=f'extract_{table}_chunk_to_gcs',
                mysql_conn_id='mysql_local',
                # Ingests bounded dates from the upstream coordinator task via XCom
                sql=f"""
                    SELECT * FROM {table} 
                    WHERE last_update >= '{{{{ task_instance.xcom_pull(task_ids="parse_dates")["window_start"] }}}}' 
                      AND last_update < '{{{{ task_instance.xcom_pull(task_ids="parse_dates")["window_end"] }}}}'
                """,
                bucket=GCS_BUCKET,
                filename=f'raw/sakila/{table}/backfill/run_{{{{ task_instance.xcom_pull(task_ids="parse_dates")["window_nodash"] }}}}/{table}_*.json',
                approx_max_file_size_bytes=10485760,
                export_format='json',
                gcp_conn_id='google_cloud_default'
            )

            load_to_bq = GCSToBigQueryOperator(
                task_id=f'load_{table}_chunk_to_bigquery',
                bucket=GCS_BUCKET,
                source_objects=[f'raw/sakila/{table}/backfill/run_{{{{ task_instance.xcom_pull(task_ids="parse_dates")["window_nodash"] }}}}/*.json'],
                destination_project_dataset_table=f'{BQ_RAW_DATASET}.raw_{table}',
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_APPEND',       # Stacks historical rows incrementally
                autodetect=True,
                time_partitioning={"type": "DAY"},
                gcp_conn_id='google_cloud_default'
            )

            extract_to_gcs >> load_to_bq

    # Task 3: Evaluate loop terminal state and route execution tree
    def evaluate_loop_state(**context):
        xcom_data = context['task_instance'].xcom_pull(task_ids='parse_dates')
        if xcom_data['loop_complete']:
            return 'invoke_dag_3_dbt'
        else:
            return 'trigger_next_historical_week'

    determine_lineage = BranchPythonOperator(
        task_id='determine_lineage',
        python_callable=evaluate_loop_state
    )

    # Branch Route A: Re-invoke self with advanced current_pointer state parameters
    trigger_next_historical_week = TriggerDagRunOperator(
        task_id='trigger_next_historical_week',
        trigger_dag_id='dag_2_historical_weekly_backfill',
        conf={
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
            "current_pointer": "{{ task_instance.xcom_pull(task_ids='parse_dates')['next_pointer'] }}"
        },
        wait_for_completion=False
    )

    # Branch Route B: Terminate state chain and step down to incremental dbt model
    invoke_dag_3_dbt = TriggerDagRunOperator(
        task_id='invoke_dag_3_dbt',
        trigger_dag_id='dag_3_dbt_transformation',     # Downstream target pipeline hook
        wait_for_completion=False,
        allowed_states=['success']
    )

    # Establish complete execution graph lineage dependencies
    parse_dates >> ingestion_group >> determine_lineage
    determine_lineage >> trigger_next_historical_week
    determine_lineage >> invoke_dag_3_dbt