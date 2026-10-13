from airflow.sdk import dag, task, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import json
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id="extract_cliente",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["exemplo2", "etl"],
)
def dag_definition():
    @task
    def execute_extract_cliente():
        variable_value = Variable.get("periodo")
        print("Executing extract_cliente")
        print('x ate y')
        variable_value = {
        "dt_ini": "15/09/2026 00:10",
        "dt_fim": "15/09/2026 00:20"
        }
        Variable.set("periodo", json.dumps(variable_value))
        return variable_value

    @task
    def execute_extract_locacao():
        print("Executing extract_cliente")
        print('x ate y')

    @task
    def execute_extract_filme():
        print("Executing extract_blockbuster")

    exec1 = execute_extract_cliente()
    exec3 = execute_extract_filme()
    exec2 = execute_extract_locacao()

    [exec1, exec3] >>  exec2

dag_definition()