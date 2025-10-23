# import sys
# import os

# # Add external script directory to sys.path
# external_script_path = '/opt/airflow/scripts'
# sys.path.append(external_script_path)

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from pendulum import duration
# from main_load_orders import main_load_orders
# from main_load_order_items import main_load_order_items

# with DAG(
#     dag_id='order_generator',
#     start_date = datetime(2025,10,4),
#     schedule= '@hourly',
#     catchup=True,
#     description='Orders data generator and load to postgres db',
#     tags=['data-loader','hourly'],
#     default_args={"retries":1},
#     dagrun_timeout=duration(minutes=20)
# )as dag:
    
#     load_orders_task = PythonOperator(
#         task_id='load_orders',
#         python_callable=main_load_orders,
#         op_kwargs={'ds':'{{ ds }}','ts': '{{ ts }}'},
#         provide_context=True
#     )
#     load_order_items_task = PythonOperator(
#         task_id='load_order_items',
#         python_callable=main_load_order_items,
#         op_kwargs={'ts': '{{ ts }}'},
#         provide_context=True
#     )