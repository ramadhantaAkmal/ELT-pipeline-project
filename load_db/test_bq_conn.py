from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
hook = BigQueryHook(gcp_conn_id='bigquery_default')
client = hook.get_client()
print(client.project)