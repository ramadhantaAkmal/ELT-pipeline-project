import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import random
from load_db.config.db_config import db_config
from load_db.utils.functions import load_schema_from_json, load_data_to_table, connect_to_db

def main(ts, **kwargs):
     # Connect to database
    engine = connect_to_db(db_config)
    if engine is None:
        return
    
    # Load schema from JSON file
    schema_file = '/opt/airflow/scripts/load_db/config/schema.json'
    schema = load_schema_from_json(schema_file)
    if schema is None:
        return
    
    # Fetch inserted category IDs
    cat_df = pd.read_sql("SELECT category_id, category_name FROM categories", engine)
    
    # Generate and load products (3 per category)
    products_rows = []
    for _, row in cat_df.iterrows():
        for i in range(6):
            product_name = f"{row['category_name']} Product {i+1}"
            description = f"A high-quality {row['category_name'].lower()} product."
            price = round(random.uniform(10.0, 500.0), 2)
            image = f"http://example.com/images/{product_name.replace(' ', '_')}.jpg"
            products_rows.append({
                'product_category_id': row['category_id'],
                'product_name': product_name,
                'product_description': description,
                'product_price': price,
                'product_image': image,
                'created_at': ts
            })
    df_products = pd.DataFrame(products_rows)
    if not load_data_to_table(engine, df_products, 'products', schema):
        return
    
if __name__ == "__main__":
    print('this script only run on airflow')
    # main()