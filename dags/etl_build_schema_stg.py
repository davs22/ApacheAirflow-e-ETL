from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'diirr',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'etl_build_schema_stg_adventureworks',
    default_args=default_args,
    description='Criação das tabelas de Staging (AdventureWorks)',
    schedule=None,
    catchup=False,
    tags=['adventureworks', 'stg', 'schema'],
) as dag:

    create_stg_tables = SQLExecuteQueryOperator(
        task_id='create_stg_tables',
        conn_id='postgres_default',
        sql='sql/stg_adventureworks.sql'
    )