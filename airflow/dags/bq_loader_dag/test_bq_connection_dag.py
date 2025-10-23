from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from datetime import datetime

with DAG(
    dag_id='test_bigquery_conn',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test_conn = BigQueryGetDatasetOperator(
        task_id='test_bq_conn',
        dataset_id='akmal_ecommerce_bronze_capstone3',
        gcp_conn_id='bigquery_default',
    )
    
    test_conn
