# Add external script directory to sys.path
external_script_path = '/opt/airflow/scripts'
sys.path.append(external_script_path)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_ingest_to_bigquery',
    default_args=default_args,
    description='Daily ingest from DB to BigQuery',
    schedule_interval='@daily',  # Jalankan daily
    catchup=False,
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

extract_task >> load_task  # Alur: extract lalu load