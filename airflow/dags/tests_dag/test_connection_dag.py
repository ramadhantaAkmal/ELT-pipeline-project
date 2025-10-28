import sys
import os

# Add external script directory to sys.path
external_script_path = '/opt/airflow/tests'
sys.path.append(external_script_path)

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract_database_test import extract_all_tables

with DAG(
    dag_id='test_conn',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test']
) as dag:
    
    test_conn = BigQueryGetDatasetOperator(
        task_id='test_bq_conn',
        dataset_id='akmal_ecommerce_bronze_capstone3',
        gcp_conn_id='bigquery_default',
    )
    
    postgres_test_conn = PythonOperator(
        task_id='test_postgres_conn',
        python_callable=extract_all_tables,
        provide_context=True,
        dag=dag,
    )
    
    test_conn >> postgres_test_conn
