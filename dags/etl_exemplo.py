from datetime import datetime, time
import json
from pathlib import Path
from airflow.sdk import dag, task


@dag(
    dag_id="etl_exemplo_persistencia_local",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["exemplo", "etl"],
)
def etl_exemplo_persistencia_local():
    @task
    def extrair():
        return [
            {"id": 1, "nome": "Produto A", "quantidade": 3},
            {"id": 2, "nome": "Produto B", "quantidade": 7},
        ]

    @task
    def transformar(registros):
        return [
            {**registro, "valor_total": registro["quantidade"] * 10}
            for registro in registros
        ]
    
    @task
    def transformar2(registros):    
        return [
            {**registro, "valor_total": registro["quantidade"] * 10}
            for registro in registros
        ]
    
    @task
    def persistir(registros):
        destino = Path("/opt/airflow/data/resultado_etl.json")
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(
            json.dumps(registros, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return str(destino)

    extracao = extrair()
    tranfor = transformar(extracao)
    tranfor2 = transformar2(extracao)
    result = persistir(tranfor)

    extracao >>[tranfor, tranfor2] >> result

etl_exemplo_persistencia_local()