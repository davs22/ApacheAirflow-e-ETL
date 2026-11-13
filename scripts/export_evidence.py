"""Exporta evidências sem credenciais. Rodar no container Airflow."""
import json
import sys
from pathlib import Path
import sqlparse
sys.path.insert(0, '/opt/airflow/dags')
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from aw_pipeline import SQL_DIR

out = {}
c = PostgresHook(postgres_conn_id='postgres_default').get_conn()
try:
    with c.cursor() as q:
        for table in ('dw_d_produto','dw_d_cliente','dw_d_territorio','dw_d_calendario','dw_f_vendas'):
            q.execute(f'SELECT COUNT(*) FROM {table}')
            out[table] = q.fetchone()[0]
        out['kpis'] = []
        for number, sql in enumerate(sqlparse.split((SQL_DIR/'kpis_adventureworks.sql').read_text(encoding='utf-8')),1):
            q.execute(sql)
            out['kpis'].append({'numero':number,'colunas':[d[0] for d in q.description],'resultados':q.fetchall()})
        q.execute('''SELECT cal.ano,cal.mes,SUM(f.valor_total) receita FROM dw_f_vendas f
            JOIN dw_d_calendario cal ON cal.sk_data=f.sk_data_pedido GROUP BY cal.ano,cal.mes ORDER BY receita DESC LIMIT 5''')
        out['meses_maior_receita'] = q.fetchall()
        q.execute("SELECT dag_id,state,run_id FROM dag_run WHERE dag_id='etl_incremental_adventureworks' ORDER BY id DESC LIMIT 3")
        out['dag_runs'] = q.fetchall()
        q.execute('''SELECT run_id,tabela,lidos,alterados FROM etl.extraction_log
            WHERE run_id LIKE 'auditoria_%' ORDER BY id''')
        out['extracoes_auditoria'] = q.fetchall()
        q.execute('''SELECT column_name,data_type,numeric_precision,numeric_scale,is_nullable FROM information_schema.columns
            WHERE table_schema='public' AND table_name='dw_f_vendas' ORDER BY ordinal_position''')
        out['tipos_fato'] = q.fetchall()
finally:
    c.close()
c = MsSqlHook(mssql_conn_id='mssql_adventureworks').get_conn()
try:
    with c.cursor() as q:
        q.execute('SELECT COUNT(*),COUNT(DISTINCT SalesOrderID),SUM(LineTotal) FROM Sales.SalesOrderDetail')
        out['origem_itens_pedidos_receita'] = q.fetchone()
finally:
    c.close()
Path('/opt/airflow/data/evidence.json').write_text(json.dumps(out,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
print(json.dumps(out,ensure_ascii=False,indent=2,default=str))
