from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import duration

DBT_DIR = "/opt/airflow/dbt_dwh_transform"

with DAG(
    dag_id="dwh_gold_layer_dag",
    start_date=datetime(2025, 10, 6),
    schedule='@weekly',
    catchup=True,
    description="Create gold layer from silver layer",
    tags=["weekly", "dwh", "gold"],
    default_args={"retries": 1},
    dagrun_timeout=duration(minutes=20)
):
    gold_layer = BashOperator(
        task_id="gold_layer",
        bash_command=f"cd {DBT_DIR} && dbt run -s akmal_ecommerce_gold_capstone3 --target gold"
    )
    
    gold_layer