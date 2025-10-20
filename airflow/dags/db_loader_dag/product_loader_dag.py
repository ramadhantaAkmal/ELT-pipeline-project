# import sys
# import os

# # Add external script directory to sys.path
# external_script_path = '/opt/airflow/scripts'
# sys.path.append(external_script_path)

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from pendulum import duration
# from main_load_products import main

# with DAG(
#     dag_id='product_generator',
#     start_date = datetime(2025,10,3),
#     schedule= '0 12,0 * * *',
#     catchup=True,
#     description='product data generator and load to postgres db',
#     tags=['data-loader','half-day'],
#     default_args={"retries":1},
#     dagrun_timeout=duration(minutes=20)
# )as dag:
#     load_products_task = PythonOperator(
#         task_id='load_products',
#         python_callable=main,
#         op_kwargs={'ts': '{{ ts }}'},
#         provide_context=True
#     )