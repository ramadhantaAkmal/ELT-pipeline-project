from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from datetime import datetime, timedelta
from load_bigquery.utils.misc_function import get_field_type

def extract_all_tables(**kwargs):
    # Ambil tanggal H-1
    execution_date = kwargs['execution_date']
    yesterday = (execution_date - timedelta(days=1)).date()
    
    pg_hook = PostgresHook(postgres_conn_id='local_postgres_default')
    tables = ['customers', 'products','orders','order_items']  
    data = {}
    
    for table in tables:
        query = f"""
            SELECT * FROM {table}
            WHERE DATE(created_at) = '{yesterday}'
        """
        df = pg_hook.get_pandas_df(query)
        data[table] = df
    
    # Push data ke XCom untuk task selanjutnya
    kwargs['ti'].xcom_push(key='extracted_data', value=data)
    return data

def load_department_categories(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='local_postgres_default')
    tables = ['departments', 'categories']
    data = {}
    
    for table in tables:
        query = f"""
            SELECT * FROM {table}
        """
        df = pg_hook.get_pandas_df(query)
        data[table] = df
    
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default', use_legacy_sql=False)
    client = bq_hook.get_client()
    project_id = 'jcdeah-006'
    dataset_id = 'akmal_ecommerce_bronze_capstone3'
    
    for table, df in data.items():
        table_id = f'{project_id}.{dataset_id}.{table}'
        table_ref = bigquery.TableReference.from_string(table_id)
        
        # Check if table already exists
        try:
            client.get_table(table_ref)
            print(f"Table {table_id} already exists. Skipping load.")
            continue  # Skip to the next table
        except Exception as e:
            if "Not found" in str(e):  # Table does not exist
                # Define schema based on DataFrame columns
                schema = []
                for column_name, dtype in df.dtypes.items():
                    field_type = get_field_type(dtype)  # Assuming get_field_type is defined
                    schema.append(bigquery.SchemaField(column_name, field_type))
                
                # Create table with schema
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
                
                # Load data
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
                )
                
                job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
                job.result()  # Wait for the job to complete
                print(f"Table {table_id} created and loaded successfully.")
            else:
                raise  # Re-raise other exceptions
    
    # for table, df in data.items():
    #     table_id = f'{project_id}.{dataset_id}.{table}'
    #     table_ref = bigquery.TableReference.from_string(table_id)
        
    #     # Schema partitioning: Partition by created_at (asumsi TIMESTAMP)
    #     schema = []
    #     table_columns = df.columns

    #     for column_name, dtype in df.dtypes.items():
    #         # Map Pandas dtype to BigQuery field type 
    #         field_type = get_field_type(dtype)
    #         schema.append(bigquery.SchemaField(column_name, field_type))
        
    #     table = bigquery.Table(table_ref, schema=schema)
        
    #     # Buat tabel jika belum ada
    #     try:
    #         client.get_table(table_ref)
    #     except:
    #         client.create_table(table)
        
    #     # Incremental load: Gunakan job untuk upsert (merge)
    #     job_config = bigquery.LoadJobConfig(
    #         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append dulu, lalu merge jika perlu
    #         schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    #     )
        
    #     # Load DF ke BigQuery
    #     job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    #     job.result()  # Tunggu selesai
        
 
    
def load_to_bigquery(**kwargs):
    # Ambil data dari XCom
    execution_date = kwargs['execution_date']
    yesterday = (execution_date - timedelta(days=1)).date()
    data = kwargs['ti'].xcom_pull(key='extracted_data')
    
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default', use_legacy_sql=False)
    client = bq_hook.get_client()
    
    project_id = 'jcdeah-006'
    dataset_id = 'akmal_ecommerce_bronze_capstone3'  
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    
    
    for table, df in data.items():
        table_id = f'{project_id}.{dataset_id}.{table}'
        table_ref = bigquery.TableReference.from_string(table_id)
        
        # Schema partitioning: Partition by created_at (asumsi TIMESTAMP)
        schema = []
        table_columns = df.columns
        primary_keys = table_columns[0]

        for column_name, dtype in df.dtypes.items():
            # Map Pandas dtype to BigQuery field type 
            field_type = get_field_type(dtype)
            schema.append(bigquery.SchemaField(column_name, field_type))
        
        partition_field = 'created_at'
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        
        # Buat tabel jika belum ada
        try:
            client.get_table(table_ref)
        except:
            client.create_table(table)
        
        # Incremental load: Gunakan job untuk upsert (merge)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append dulu, lalu merge jika perlu
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        
        # Load DF ke BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Tunggu selesai
        
        columns = ', '.join(df.columns)
        update_columns = ', '.join([f'T.{col} = S.{col}' for col in df.columns if col != primary_keys])
        insert_clause = f"INSERT ({columns}) VALUES ({', '.join([f'S.{col}' for col in df.columns])})"
        
        # Untuk true upsert (merge), jalankan query merge post-load
        merge_query = f"""
            MERGE `{table_id}` T
            USING (SELECT {columns} FROM `{table_id}` WHERE DATE(created_at) = '{yesterday}') S
            ON T.{primary_keys} = S.{primary_keys}
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
            WHEN NOT MATCHED THEN
                {insert_clause}
        """
        client.query(merge_query).result()  # Jalankan merge untuk incremental
        
