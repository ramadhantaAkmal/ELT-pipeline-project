import pandas as pd
from sqlalchemy import create_engine
import random
from datetime import datetime, timedelta
from config.db_config import db_config
from utils.functions import load_schema_from_json, load_data_to_table, connect_to_db

def main_load_orders(ds, ts, **kwargs):
    
    # Connect to database
    engine = connect_to_db(db_config)
    if engine is None:
        return
    
    # Load schema from JSON file
    schema_file = '/opt/airflow/scripts/config/schema.json'
    schema = load_schema_from_json(schema_file)
    if schema is None:
        return
    
    # Fetch inserted customer IDs
    customer_ids = pd.read_sql("SELECT customer_id FROM customers", engine)['customer_id'].tolist()
    
    # Generate and load orders (20 orders)
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    order_rows = []
    # for i in range(1):
    customer_id = random.choice(customer_ids)
    status = random.choice(statuses)
    order_rows.append({
        'order_date': ds,
        'order_customer_id': customer_id,
        'order_status': status,
        'created_at': ts
    })
    df_orders = pd.DataFrame(order_rows)
    if not load_data_to_table(engine, df_orders, 'orders', schema):
        return

if __name__ == "__main__":
    print('this script only run on airflow')
    # main()