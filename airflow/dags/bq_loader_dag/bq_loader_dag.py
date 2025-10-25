import sys
import os

# Add external script directory to sys.path
external_script_path = '/opt/airflow/scripts'
sys.path.append(external_script_path)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from pendulum import duration
import pandas as pd
from load_bigquery.main_load_to_bq import extract_all_tables, load_to_bigquery, load_department_categories

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'daily_ingest_to_bigquery',
    default_args=default_args,
    description='Daily ingest from DB to BigQuery',
    schedule_interval='@daily',  
    catchup=True,
    dagrun_timeout=duration(minutes=20),
    tags=['bigquery-loader','daily']
)

initialize_bq_task = PythonOperator(
    task_id='init_bigquery',
    python_callable=load_department_categories,
    provide_context=True,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_all_tables',
    python_callable=extract_all_tables,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    provide_context=True,
    dag=dag,
)

initialize_bq_task >> extract_task >> load_task  