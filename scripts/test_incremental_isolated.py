"""Integração destrutiva APENAS em bancos temporários criados pelo próprio teste.

Executar no container Airflow. Clona as nove tabelas lidas, sem tocar na origem real.
Os nomes únicos tornam a repetição independente de execuções anteriores.
"""
import json
import os
import sys
import uuid
from pathlib import Path
sys.path.insert(0, '/opt/airflow/dags')
from aw_pipeline import SOURCES, extract_table, initialize_dw, run_sql
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

suffix = uuid.uuid4().hex[:10]
ms_db, pg_db = 'aw_audit_' + suffix, 'aw_audit_' + suffix
ms_config = os.environ['AIRFLOW_CONN_MSSQL_ADVENTUREWORKS']
pg_config = os.environ['AIRFLOW_CONN_POSTGRES_DEFAULT']
admin_ms = MsSqlHook(mssql_conn_id='mssql_adventureworks').get_conn()
admin_pg = PostgresHook(postgres_conn_id='postgres_default').get_conn()
admin_ms.autocommit(True)
admin_pg.autocommit = True
results = []
created_ms = created_pg = False


def check(name, ok, detail=None):
    results.append({'teste': name, 'aprovado': bool(ok), 'detalhe': detail})
    print(results[-1], flush=True)
    assert ok, name


def pg(sql, params=None):
    c = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    try:
        with c, c.cursor() as q:
            q.execute(sql, params)
            return q.fetchall() if q.description else None
    finally:
        c.close()


def ms(sql):
    c = MsSqlHook(mssql_conn_id='mssql_adventureworks').get_conn()
    try:
        with c.cursor() as q:
            q.execute(sql)
            out = q.fetchall() if q.description else None
        c.commit()
        return out
    finally:
        c.close()


def pipeline(label):
    run_sql('stg_adventureworks.sql')
    initialize_dw()
    for t in SOURCES:
        extract_table(t, run_id=label)
    run_sql('load_dimensions.sql')
    run_sql('load_facts.sql')
    run_sql('validate_dw.sql')


try:
    with admin_ms.cursor() as q:
        q.execute(f'CREATE DATABASE [{ms_db}]')
        created_ms = True
        for schema in ('Production', 'Sales', 'Person'):
            q.execute(f'EXEC [{ms_db}].sys.sp_executesql N\'CREATE SCHEMA [{schema}]\'')
        for table in ('Production.ProductCategory', 'Production.ProductSubcategory', 'Production.Product',
                      'Sales.SalesTerritory', 'Sales.Customer', 'Sales.SalesOrderHeader',
                      'Sales.SalesOrderDetail', 'Person.Person', 'Sales.Store'):
            columns = {'Person.Person': 'BusinessEntityID,FirstName,MiddleName,LastName,ModifiedDate',
                       'Sales.Store': 'BusinessEntityID,Name,ModifiedDate'}.get(table, '*')
            q.execute(f'SELECT {columns} INTO [{ms_db}].{table} FROM AdventureWorks2019.{table}')
    with admin_pg.cursor() as q:
        q.execute(f'CREATE DATABASE {pg_db}')
        created_pg = True
    for name, config, db in [('AIRFLOW_CONN_MSSQL_ADVENTUREWORKS', ms_config, ms_db),
                              ('AIRFLOW_CONN_POSTGRES_DEFAULT', pg_config, pg_db)]:
        config = json.loads(config)
        config['schema'] = db
        os.environ[name] = json.dumps(config)

    pipeline('isolado_bootstrap')
    check('Bootstrap em banco vazio: 121317 itens', pg('SELECT COUNT(*) FROM dw_f_vendas')[0][0] == 121317)
    # Item novo + território nulo: deve usar o membro desconhecido, sem perder venda.
    header = ms('SELECT TOP 1 SalesOrderID FROM Sales.SalesOrderHeader ORDER BY SalesOrderID')[0][0]
    ms(f'UPDATE Sales.SalesOrderHeader SET TerritoryID=NULL,ModifiedDate=GETDATE() WHERE SalesOrderID={header}')
    new_id = ms(f'''INSERT INTO Sales.SalesOrderDetail
        (SalesOrderID,CarrierTrackingNumber,OrderQty,ProductID,SpecialOfferID,UnitPrice,UnitPriceDiscount,LineTotal,rowguid,ModifiedDate)
        OUTPUT INSERTED.SalesOrderDetailID
        SELECT TOP 1 SalesOrderID,CarrierTrackingNumber,2,ProductID,SpecialOfferID,UnitPrice,0,2*UnitPrice,NEWID(),GETDATE()
        FROM Sales.SalesOrderDetail WHERE SalesOrderID={header}''')[0][0]
    pipeline('isolado_insert')
    check('Inserção incremental de item novo', pg('SELECT COUNT(*) FROM dw_f_vendas')[0][0] == 121318)
    check('Território ausente preserva venda', pg('''SELECT t.territory_id FROM dw_f_vendas f
        JOIN dw_d_territorio t USING(sk_territorio) WHERE f.sales_order_detail_id=%s''', (new_id,))[0][0] == -1)
    sk = pg('SELECT sk_venda FROM dw_f_vendas WHERE sales_order_detail_id=%s', (new_id,))[0][0]
    ms(f'UPDATE Sales.SalesOrderDetail SET OrderQty=3,LineTotal=3*UnitPrice,ModifiedDate=GETDATE() WHERE SalesOrderDetailID={new_id}')
    pipeline('isolado_update')
    row = pg('SELECT sk_venda,quantidade FROM dw_f_vendas WHERE sales_order_detail_id=%s', (new_id,))[0]
    check('Atualização incremental preserva chave substituta', row == (sk, 3), list(row))
    # Mudar somente cabeçalho, inclusive para uma data fora do calendário inicial.
    ms(f"UPDATE Sales.SalesOrderHeader SET OrderDate='2030-01-02',ModifiedDate=GETDATE() WHERE SalesOrderID={header}")
    pipeline('isolado_header')
    check('Mudança só no cabeçalho e expansão do calendário', pg('SELECT sk_data_pedido FROM dw_f_vendas WHERE sales_order_detail_id=%s', (new_id,))[0][0] == 20300102)
    ms("UPDATE Production.ProductCategory SET Name='Categoria auditada',ModifiedDate=GETDATE() WHERE ProductCategoryID=1")
    pipeline('isolado_categoria')
    check('Categoria alterada propaga para produtos sem alterar Product', pg("SELECT COUNT(*) FROM dw_d_produto WHERE categoria='Categoria auditada'")[0][0] > 0)
    # Falha no INSERT deve reverter tanto staging quanto watermark.
    watermark = pg("SELECT watermark FROM etl.watermark WHERE tabela='productcategory'")[0][0]
    ms("UPDATE Production.ProductCategory SET Name='Falha transacional',ModifiedDate=GETDATE() WHERE ProductCategoryID=1")
    pg("ALTER TABLE stg.productcategory ADD CONSTRAINT rejeitar_teste CHECK(name<>'Falha transacional')")
    failed = False
    try:
        extract_table('productcategory', run_id='isolado_falha')
    except Exception:
        failed = True
    check('Falha não avança watermark nem altera staging', failed and
          pg("SELECT watermark FROM etl.watermark WHERE tabela='productcategory'")[0][0] == watermark and
          pg('SELECT name FROM stg.productcategory WHERE productcategoryid=1')[0][0] == 'Categoria auditada')
    pg('ALTER TABLE stg.productcategory DROP CONSTRAINT rejeitar_teste')
    pipeline('isolado_retomada')
    check('Retomada após falha recupera alteração', pg('SELECT name FROM stg.productcategory WHERE productcategoryid=1')[0][0] == 'Falha transacional')
    before = pg('SELECT COUNT(*),MAX(data_carga) FROM dw_f_vendas')[0]
    pipeline('isolado_repeticao')
    check('Repetição com janela sobreposta não duplica nem atualiza fatos', pg('SELECT COUNT(*),MAX(data_carga) FROM dw_f_vendas')[0] == before)
    pg('TRUNCATE dw_f_vendas')  # Banco isolado: exercita os dez KPIs sem denominador.
    run_sql('kpis_adventureworks.sql')
    check('Dez KPIs executam com fato vazio, sem divisão por zero', True)
finally:
    os.environ['AIRFLOW_CONN_MSSQL_ADVENTUREWORKS'] = ms_config
    os.environ['AIRFLOW_CONN_POSTGRES_DEFAULT'] = pg_config
    # Remove apenas os bancos únicos criados neste processo; nunca toca na origem real.
    if created_ms:
        with admin_ms.cursor() as q:
            q.execute(f'ALTER DATABASE [{ms_db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{ms_db}]')
    if created_pg:
        with admin_pg.cursor() as q:
            q.execute(f'DROP DATABASE {pg_db} WITH (FORCE)')
    admin_ms.close()
    admin_pg.close()
    Path('/opt/airflow/data/isolated_results.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
