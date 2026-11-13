from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from aw_pipeline import SOURCES, extract_table, initialize_dw, run_sql

with DAG('etl_incremental_adventureworks', start_date=datetime(2026, 1, 1),
         schedule='@daily', catchup=False, max_active_runs=1,
         default_args={'owner': 'diirr', 'retries': 1, 'retry_delay': timedelta(minutes=1)},
         description='AdventureWorks: origem incremental, staging, estrela e 10 KPIs',
         tags=['adventureworks', 'incremental']) as dag:
    stg = PythonOperator(task_id='create_staging', python_callable=run_sql, op_args=['stg_adventureworks.sql'])
    dw = PythonOperator(task_id='create_dw', python_callable=initialize_dw)
    stg >> dw
    extracts = []
    for table in SOURCES:
        task = PythonOperator(task_id=f'extract_{table}', python_callable=extract_table, op_kwargs={'table_name': table})
        dw >> task
        extracts.append(task)
    dims = PythonOperator(task_id='load_dimensions', python_callable=run_sql, op_args=['load_dimensions.sql'])
    facts = PythonOperator(task_id='load_facts', python_callable=run_sql, op_args=['load_facts.sql'])
    kpis = PythonOperator(task_id='calculate_10_kpis', python_callable=run_sql, op_args=['kpis_adventureworks.sql'])
    extracts >> dims >> facts >> kpis
