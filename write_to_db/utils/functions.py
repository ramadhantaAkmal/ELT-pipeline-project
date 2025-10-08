from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import json
import os

def connect_to_db(db_config):
    """Create a connection to the PostgreSQL database."""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def load_data_to_table(engine, df, table_name, schema):
    """Load DataFrame to PostgreSQL table with schema validation."""
    try:
        # Map schema data types to pandas/SQLAlchemy types
        dtype_mapping = {
            'integer': 'int32',
            'string': 'object',
            'float': 'float64',
            'timestamp': 'datetime64[ns]'
        }
        
        # Map JSON data types to PostgreSQL types for SERIAL data type detection
        pg_type_mapping = {
            'integer': 'integer',
            'string': 'varchar',
            'float': 'numeric',
            'timestamp': 'timestamp'
        }
        
        # Get schema for the table
        table_schema = next((t for t in schema if t['table_name'] == table_name), None)
        if not table_schema:
            print(f"No schema found for table {table_name}")
            return False
        
        # Create dtype dictionary for pandas
        for col in table_schema['columns']:
            col_name = col['column_name']
            pg_data_type=''
            json_data_type = col['data_type'].lower()
            if json_data_type != 'serial':
                pg_data_type = pg_type_mapping.get(json_data_type, 'varchar')
            else :
                pg_data_type = json_data_type
            # Skip SERIAL columns (auto-incremented by PostgreSQL)
            if pg_data_type == 'serial':
                if col_name in df.columns:
                    df = df.drop(columns=[col_name])
                continue
            else:
                # Check if column name is in dataframe
                if col_name not in df.columns:
                    print(f"Column {col_name} not found in DataFrame for table {table_name}")
                    return False
           
            # Map schema data type to pandas data type
            pandas_type = dtype_mapping.get(json_data_type, 'object')
            try:
                if pandas_type == 'datetime64[ns]':
                    df[col_name] = pd.to_datetime(df[col_name])
                else:
                    df[col_name] = df[col_name].astype(pandas_type)
            except Exception as e:
                print(f"Error converting column {col_name} to {pandas_type}: {e}")
                return False
        
        # Load data to PostgreSQL
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        print(f"Successfully loaded data to {table_name}")
        return True
    except Exception as e:
        print(f"Error loading data to {table_name}: {e}")
        return False
    
def load_schema_from_json(file_path):
    """Load schema from JSON file."""
    try:
        if not os.path.exists(file_path):
            print(f"Schema file {file_path} not found.")
            return None
        with open(file_path, 'r') as f:
            schema_json = json.load(f)
        
        # Convert JSON schema to the required format
        schema = []
        for table_name, columns in schema_json.items():
            schema.append({
                'table_name': table_name,
                'columns': [
                    {
                        'column_name': col['column_name'],
                        'data_type': col['data_type'] if col['data_type'] else 'varchar'  # Default to varchar if empty
                    } for col in columns
                ]
            })
        return schema
    except Exception as e:
        print(f"Error reading schema from {file_path}: {e}")
        return None