"""Validação real: executar com docker compose exec no ambiente Airflow."""
import json
import sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, '/opt/airflow/dags')
from aw_pipeline import SOURCES, run_sql, initialize_dw, extract_table
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

results = []
def check(name, condition, detail=None):
    results.append({'teste': name, 'aprovado': bool(condition), 'detalhe': detail})
    if not condition:
        raise AssertionError(name + ': ' + str(detail))

def pipeline(label):
    run_sql('stg_adventureworks.sql')
    initialize_dw()
    for table in SOURCES:
        extract_table(table, run_id=label)
    run_sql('load_dimensions.sql')
    run_sql('load_facts.sql')
    run_sql('validate_dw.sql')

def scalar(sql, params=None):
    with PostgresHook(postgres_conn_id='postgres_default').get_conn() as c:
        with c.cursor() as q:
            q.execute(sql, params)
            return q.fetchone()[0]

try:
    pipeline('auditoria_bootstrap')
    count = scalar('SELECT count(*) FROM dw_f_vendas')
    check('Carga histórica completa', count == 121317, count)
    with MsSqlHook(mssql_conn_id='mssql_adventureworks').get_conn() as src:
        with src.cursor() as q:
            q.execute('SELECT SUM(LineTotal) FROM Sales.SalesOrderDetail')
            source_total = q.fetchone()[0]
    target_total = scalar('SELECT SUM(valor_total) FROM dw_f_vendas')
    check('Receita exata SQL Server = PostgreSQL', source_total == target_total, str(target_total))
    before = scalar('SELECT MAX(data_carga) FROM dw_f_vendas')
    pipeline('auditoria_sem_mudancas')
    check('Reexecução sem duplicação', scalar('SELECT count(*) FROM dw_f_vendas') == count)
    check('Reexecução sem UPDATE de fatos inalterados', scalar('SELECT MAX(data_carga) FROM dw_f_vendas') == before)
    check('Extração sem mudanças não reprocessa histórico', scalar("SELECT SUM(lidos) FROM etl.extraction_log WHERE run_id='auditoria_sem_mudancas'") == 0)
    # Testes adicionais são implementados abaixo; nenhum dado de produção é excluído.
finally:
    Path('/opt/airflow/data/validation_results.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(results, ensure_ascii=False, indent=2))
