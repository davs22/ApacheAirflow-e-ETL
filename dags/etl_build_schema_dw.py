from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

@dag(
    dag_id="etl_build_dw",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["exemplo2", "etl", "postgres"],
)
def etl_build_dw():

    create_d_calendario = SQLExecuteQueryOperator(
        task_id="create_d_calendario",
        conn_id="sample_postgres_dw",
        sql="sql/dw_d_calendario.sql",
    )

    create_d_filme = SQLExecuteQueryOperator(
        task_id="create_d_filme",
        conn_id="sample_postgres_dw",
        sql="sql/dw_d_filme.sql",
    )

    create_d_cliente = SQLExecuteQueryOperator(
        task_id="create_d_cliente",
        conn_id="sample_postgres_dw",
        sql="sql/dw_d_cliente.sql",
    )

    create_d_midia = SQLExecuteQueryOperator(
        task_id="create_d_midia",
        conn_id="sample_postgres_dw",
        sql="sql/dw_d_midia.sql",
    )

    create_d_locacao = SQLExecuteQueryOperator(
        task_id="create_d_locacao",
        conn_id="sample_postgres_dw",
        sql="sql/dw_d_locacao.sql",
    )

    create_f_emprestimo = SQLExecuteQueryOperator(
        task_id="create_f_emprestimo",
        conn_id="sample_postgres_dw",
        sql="sql/dw_f_emprestimo.sql",
    )

    create_f_locacao = SQLExecuteQueryOperator(
        task_id="create_f_locacao",
        conn_id="sample_postgres_dw",
        sql="sql/dw_f_locacao.sql",
    )

    [create_d_calendario, create_d_filme, create_d_cliente, create_d_midia, create_d_locacao] >> create_f_emprestimo
    
    [create_d_calendario, create_d_filme, create_d_cliente, create_d_midia, create_d_locacao] >> create_f_locacao

etl_build_dw()

