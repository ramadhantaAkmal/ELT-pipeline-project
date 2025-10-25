import pandas as pd
from sqlalchemy import create_engine
import random
from datetime import datetime, timedelta
from load_db.config.db_config import db_config
from load_db.utils.functions import load_schema_from_json, load_data_to_table, connect_to_db

def main_load_order_items(ts, **kwargs):
    
     # Connect to database
    engine = connect_to_db(db_config)
    if engine is None:
        return
    
    # Load schema from JSON file
    schema_file = '/opt/airflow/scripts/load_db/config/schema.json'
    schema = load_schema_from_json(schema_file)
    if schema is None:
        return
    
    # Fetch inserted order IDs and product details
    order_ids = pd.read_sql("SELECT order_id FROM orders", engine)['order_id'].tolist()
    products_df = pd.read_sql("SELECT product_id, product_price FROM products", engine)
    product_ids = products_df['product_id'].tolist()
    product_prices = dict(zip(products_df['product_id'], products_df['product_price']))
    
    # Generate and load order_items (1-8 items per order)
    order_items_rows = []
    for order_id in order_ids:
        num_items = random.randint(1, 8)
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            price = product_prices[product_id]
            subtotal = quantity * price
            order_items_rows.append({
                'order_item_order_id': order_id,
                'order_item_product_id': product_id,
                'order_item_quantity': quantity,
                'order_item_subtotal': subtotal,
                'order_item_product_price': price,
                'created_at': ts
            })
    df_order_items = pd.DataFrame(order_items_rows)
    if not load_data_to_table(engine, df_order_items, 'order_items', schema):
        return

if __name__ == "__main__":
    print('this script only run on airflow')