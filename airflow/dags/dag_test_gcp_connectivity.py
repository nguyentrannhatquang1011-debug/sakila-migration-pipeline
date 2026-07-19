# airflow/dags/dag_test_gcp_connectivity.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

GCS_BUCKET = 'sakila-landing-zone-quang-2026'
CONN_ID = 'google_cloud_default'

default_args = {
    'owner': 'senior_mentor',
    'depends_on_past': False,
    'retries': 0,
}

def test_gcs_bucket_connectivity():
    """Validates read and write permission boundaries inside the target GCS Bucket."""
    print(f"--- COMMENCING GCS HOOK TEST TARGETING BUCKET: {GCS_BUCKET} ---")
    hook = GCSHook(gcp_conn_id=CONN_ID)
    
    # FIXED: Replaced exists() with list() to correctly test bucket presence and read access
    try:
        hook.list(bucket_name=GCS_BUCKET, max_results=1)
        print(f"SUCCESS: Target bucket '{GCS_BUCKET}' is reachable and readable.")
    except Exception as error:
        raise ValueError(f"CRITICAL: Target bucket '{GCS_BUCKET}' is unreachable. Access Denied. Error: {error}")
    
    # Perform a lightweight write test by dropping a dynamic verification text token
    test_blob_name = "connectivity_tests/smoke_test_token.txt"
    hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=test_blob_name,
        data="STATUS=OK; TIMESTAMP=" + datetime.now().isoformat(),
        mime_type="text/plain"
    )
    print(f"SUCCESS: Handshake token successfully deployed to GCS at: {test_blob_name}")

def test_bigquery_cluster_connectivity():
    """Validates execution pipelines and compute handshakes inside BigQuery."""
    print("--- COMMENCING BIGQUERY HOOK TEST RUN ---")
    hook = BigQueryHook(gcp_conn_id=CONN_ID)
    
    test_sql = "SELECT 1 AS connectivity_check;"
    client = hook.get_client()
    query_job = client.query(test_sql)
    results = list(query_job.result())
    
    if len(results) > 0 and results[0]['connectivity_check'] == 1:
        print("SUCCESS: BigQuery engine successfully parsed and returned the evaluation query matrix.")
    else:
        raise ValueError("CRITICAL: BigQuery connectivity test failed to process evaluation matrix.")

with DAG(
    dag_id='dag_test_gcp_connectivity',
    default_args=default_args,
    description='Infrastructure smoke-test DAG designed to validate credential visibility for GCS and BQ',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    max_active_runs=3,                                 # HARD REQUIREMENT: Limit parallel runs to 3
    catchup=False,
    tags=['validation', 'smoke_test', 'gcp']
) as dag:

    verify_gcs_link = PythonOperator(
        task_id='validate_gcs_storage_bucket',
        python_callable=test_gcs_bucket_connectivity
    )

    verify_bq_link = PythonOperator(
        task_id='validate_bigquery_engine',
        python_callable=test_bigquery_cluster_connectivity
    )

    [verify_gcs_link, verify_bq_link]