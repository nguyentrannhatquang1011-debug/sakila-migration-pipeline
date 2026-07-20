# airflow/dags/dag_1_generate_data.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from sakila_generator import run_comprehensive_generation

# Standard system operation defaults
default_args = {
    'owner': 'senior_mentor',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_1_generate_data',
    default_args=default_args,
    description='Triggers synthetic transactions and master record updates inside target OLTP source',
    schedule='@daily',
    start_date=datetime(2026, 7, 1), # year, month, day
    max_active_runs=3,             # HARD REQUIREMENT: Limits parallel execution instances to 3
    catchup=True,
    tags=['production_simulation', 'sakila']
) as dag:

    # Task to invoke the Python data generation engine
    execute_generation = PythonOperator(
        task_id='data_generation_task',
        python_callable=run_comprehensive_generation,
        op_kwargs={'mysql_conn_id': 'mysql_local'}
    )

    # Task to invoke the downstream migration workflow
    trigger_downstream_migration = TriggerDagRunOperator(
        task_id='invoke_dag_2_migration',
        trigger_dag_id='dag_2_mysql_to_bigquery',
        wait_for_completion=False
    )

    # Establish directional graph execution lineage
    execute_generation >> trigger_downstream_migration