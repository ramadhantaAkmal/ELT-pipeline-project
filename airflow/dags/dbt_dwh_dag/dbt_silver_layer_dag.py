from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import duration

DBT_DIR = "/opt/airflow/dbt_dwh_transform"

with DAG(
    dag_id="dwh_silver_layer_dag",
    start_date=datetime(2025, 10, 30),
    schedule='@daily',
    catchup=True,
    description="Create silver layer from bronze layer",
    tags=["daily", "dwh", "silver"],
    default_args={"retries": 1},
    dagrun_timeout=duration(minutes=20)
):
    
    silver_layer = BashOperator(
        task_id="silver_layer",
        bash_command=f"cd {DBT_DIR} && dbt run -s akmal_ecommerce_silver_capstone3 --target silver"
    )
    
    silver_layer