from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

def extract_all_tables(**kwargs):
    # Ambil tanggal H-1
    execution_date = kwargs['execution_date']
    yesterday = (execution_date - timedelta(days=1)).date()
    
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    tables = ['customers', 'products', 'purchase']  # Dari gambar
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

def load_to_bigquery(**kwargs):
    # Ambil data dari XCom
    data = kwargs['ti'].xcom_pull(key='extracted_data')
    
    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default', use_legacy_sql=False)
    client = bq_hook.get_client()
    
    project_id = 'jcdeah-006'
    dataset_id = 'akmal_ecommerce_bronze_capstone3'  # Contoh dari kriteria
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    
    # Pastikan dataset ada (buat jika belum)
    try:
        client.get_dataset(dataset_ref)
    except:
        client.create_dataset(dataset_ref)
    
    for table, df in data.items():
        table_id = f'{project_id}.{dataset_id}.{table}'
        table_ref = bigquery.TableReference.from_string(table_id)
        
        # Schema partitioning: Partition by created_at (asumsi TIMESTAMP)
        schema = []  # Infer schema dari DF, atau definisikan manual jika perlu
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
        # Asumsi primary key 'id'; sesuaikan jika beda
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append dulu, lalu merge jika perlu
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        
        # Load DF ke BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Tunggu selesai
        
        # Untuk true upsert (merge), jalankan query merge post-load
        merge_query = f"""
            MERGE `{table_id}` T
            USING (SELECT * FROM `{table_id}` WHERE DATE(created_at) = '{yesterday}') S
            ON T.id = S.id  # Asumsi PK 'id'
            WHEN MATCHED THEN
                UPDATE SET 
                    -- Update fields here, e.g. T.column1 = S.column1, etc.
            WHEN NOT MATCHED THEN
                INSERT (*) VALUES (*)
        """
        client.query(merge_query).result()  # Jalankan merge untuk incremental