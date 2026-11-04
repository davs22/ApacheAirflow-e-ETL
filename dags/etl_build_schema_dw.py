from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'diirr',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'etl_build_schema_dw_adventureworks',
    default_args=default_args,
    description='Criação do Star Schema do Data Warehouse (AdventureWorks)',
    schedule=None,
    catchup=False,
    tags=['adventureworks', 'dw', 'schema', 'demonstracao_competencia']   
) as dag:

    create_dim_calendario = SQLExecuteQueryOperator(
        task_id='create_dim_calendario',
        conn_id='postgres_default',
        sql='sql/dw_d_calendario.sql'
    )

    create_dim_produto = SQLExecuteQueryOperator(
        task_id='create_dim_produto',
        conn_id='postgres_default',
        sql='sql/dw_d_produto.sql'
    )

    create_dim_territorio = SQLExecuteQueryOperator(
        task_id='create_dim_territorio',
        conn_id='postgres_default',
        sql='sql/dw_d_territorio.sql'
    )

    create_dim_cliente = SQLExecuteQueryOperator(
        task_id='create_dim_cliente',
        conn_id='postgres_default',
        sql='sql/dw_d_cliente.sql'
    )

    create_fato_vendas = SQLExecuteQueryOperator(
        task_id='create_fato_vendas',
        conn_id='postgres_default',
        sql='sql/dw_f_vendas.sql'
    )

    [create_dim_calendario, create_dim_produto, create_dim_territorio, create_dim_cliente] >> create_fato_vendas
