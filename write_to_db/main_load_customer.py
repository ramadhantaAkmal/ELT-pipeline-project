import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import random
from config.db_config import db_config
from utils.functions import load_schema_from_json, load_data_to_table, connect_to_db

def main(ts, **kwargs):
    # Connect to database
    engine = connect_to_db(db_config)
    if engine is None:
        return
    
    # Load schema from JSON file
    schema_file = 'config/schema.json'
    schema = load_schema_from_json(schema_file)
    if schema is None:
        return
    
    # Generate and load customers (10 customers)
    fnames = ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry']
    lnames = ['Doe', 'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']
    streets = ['Main', 'Oak', 'Pine', 'Maple', 'Cedar']
    customer_rows = []
    for i in range(10):
        fname = random.choice(fnames)
        lname = random.choice(lnames)
        email = f"{fname.lower()}.{lname.lower()}@example.com"
        password = "password123"
        street = f"{random.randint(100, 9999)} {random.choice(streets)} St"
        city = random.choice(cities)
        state = random.choice(states)
        zipcode = random.randint(10000, 99999)
        customer_rows.append({
            'customer_fname': fname,
            'customer_lname': lname,
            'customer_email': email,
            'customer_password': password,
            'customer_street': street,
            'customer_city': city,
            'customer_state': state,
            'customer_zipcode': zipcode,
            'created_at': ts
        })
    df_customers = pd.DataFrame(customer_rows)
    if not load_data_to_table(engine, df_customers, 'customers', schema):
        return
    
if __name__ == "__main__":
    print('this loader only run on airflow')
    # main()