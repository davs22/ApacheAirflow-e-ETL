from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from aw_pipeline import SOURCES, extract_table, run_sql

with DAG('etl_extract_mssql_adventureworks', start_date=datetime(2026, 1, 1),
         schedule=None, catchup=False, max_active_runs=1,
         default_args={'retries': 1, 'retry_delay': timedelta(minutes=1)},
         tags=['adventureworks', 'manutencao']) as dag:
    schema = PythonOperator(task_id='create_staging', python_callable=run_sql, op_args=['stg_adventureworks.sql'])
    for table in SOURCES:
        schema >> PythonOperator(task_id=f'extract_{table}', python_callable=extract_table, op_kwargs={'table_name': table})
