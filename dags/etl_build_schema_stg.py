from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id="etl_build_stage",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["exemplo2", "etl", "postgres"],
)
def etl_build_stage():

    create_stg_cliente = SQLExecuteQueryOperator(
        task_id="create_stg_cliente",
        conn_id="sample_postgres",
        sql="sql/stg_cliente.sql",
    )

    create_stg_classificacao = SQLExecuteQueryOperator(
        task_id="create_stg_classificacao",
        conn_id="sample_postgres",
        sql="sql/stg_classificacao.sql",
    )

    create_stg_ator = SQLExecuteQueryOperator(
        task_id="create_stg_ator",
        conn_id="sample_postgres",
        sql="sql/stg_ator.sql",
    )

    create_stg_filme = SQLExecuteQueryOperator(
        task_id="create_stg_filme",
        conn_id="sample_postgres",
        sql="sql/stg_filme.sql",
    )

    create_stg_midia = SQLExecuteQueryOperator(
        task_id="create_stg_midia",
        conn_id="sample_postgres",
        sql="sql/stg_midia.sql",
    )

    create_stg_emprestimo = SQLExecuteQueryOperator(
        task_id="create_stg_emprestimo",
        conn_id="sample_postgres",
        sql="sql/stg_emprestimo.sql",
    )

    create_stg_estrela = SQLExecuteQueryOperator(
        task_id="create_stg_estrela",
        conn_id="sample_postgres",
        sql="sql/stg_estrela.sql",
    )

    dummy_operation = EmptyOperator(task_id="dummy_operation")

    [create_stg_cliente, create_stg_classificacao, create_stg_ator, create_stg_filme, create_stg_midia, create_stg_emprestimo, create_stg_estrela] >> dummy_operation
etl_build_stage()

