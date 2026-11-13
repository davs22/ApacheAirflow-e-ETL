from datetime import timedelta
from contextlib import closing
from pathlib import Path
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlparse

SQL_DIR = Path(__file__).parent / 'sql'
SOURCES = {
    'productcategory': ('productcategoryid', 'SELECT ProductCategoryID productcategoryid, Name name, ModifiedDate modifieddate FROM Production.ProductCategory'),
    'productsubcategory': ('productsubcategoryid', 'SELECT ProductSubcategoryID productsubcategoryid, ProductCategoryID productcategoryid, Name name, ModifiedDate modifieddate FROM Production.ProductSubcategory'),
    'product': ('productid', 'SELECT ProductID productid, Name name, ProductNumber productnumber, ProductSubcategoryID productsubcategoryid, StandardCost standardcost, ModifiedDate modifieddate FROM Production.Product'),
    'salesterritory': ('territoryid', 'SELECT TerritoryID territoryid, Name name, CountryRegionCode countryregioncode, [Group] [group], ModifiedDate modifieddate FROM Sales.SalesTerritory'),
    'customer': ('customerid', """SELECT c.CustomerID customerid, c.PersonID personid, c.StoreID storeid,
        c.TerritoryID territoryid,
        COALESCE(s.Name, NULLIF(LTRIM(RTRIM(CONCAT(p.FirstName, ' ', p.MiddleName, ' ', p.LastName))), ''), 'Cliente sem nome') nome_cliente,
        (SELECT MAX(v.dt) FROM (VALUES(c.ModifiedDate),(p.ModifiedDate),(s.ModifiedDate)) v(dt)) modifieddate
        FROM Sales.Customer c LEFT JOIN Person.Person p ON p.BusinessEntityID=c.PersonID
        LEFT JOIN Sales.Store s ON s.BusinessEntityID=c.StoreID"""),
    'salesorderheader': ('salesorderid', 'SELECT SalesOrderID salesorderid, OrderDate orderdate, CustomerID customerid, TerritoryID territoryid, ModifiedDate modifieddate FROM Sales.SalesOrderHeader'),
    'salesorderdetail': ('salesorderdetailid', 'SELECT SalesOrderID salesorderid, SalesOrderDetailID salesorderdetailid, OrderQty orderqty, ProductID productid, UnitPrice unitprice, UnitPriceDiscount unitpricediscount, LineTotal linetotal, ModifiedDate modifieddate FROM Sales.SalesOrderDetail'),
}


def run_sql(filename):
    with closing(PostgresHook(postgres_conn_id='postgres_default').get_conn()) as conn:
        with conn, conn.cursor() as cur:
            for statement in sqlparse.split((SQL_DIR / filename).read_text(encoding='utf-8')):
                cur.execute(statement)
                if cur.description:
                    print(cur.fetchall())


def extract_table(table_name, **context):
    key, query = SOURCES[table_name]
    pg = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    source = MsSqlHook(mssql_conn_id='mssql_adventureworks').get_conn()
    try:
        with pg:
            with pg.cursor() as target, source.cursor() as cur:
                target.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (table_name,))
                target.execute('SELECT watermark FROM etl.watermark WHERE tabela=%s FOR UPDATE', (table_name,))
                previous = target.fetchone()
                lower = previous[0] - timedelta(days=1) if previous else None
                cur.execute('SELECT GETDATE()')
                upper = cur.fetchone()[0]
                if previous and upper < previous[0]:
                    raise ValueError('Relógio da origem retrocedeu; watermark preservado')
                sql = f'SELECT * FROM ({query}) src WHERE modifieddate <= %s'
                params = [upper]
                if lower is not None:
                    sql += ' AND modifieddate >= %s'
                    params.append(lower)
                cur.execute(sql, tuple(params))
                columns = [d[0].lower() for d in cur.description]
                quoted = ','.join(f'"{c}"' for c in columns)
                updates = ','.join(f'"{c}"=EXCLUDED."{c}"' for c in columns if c != key)
                distinct = ','.join(f't."{c}"' for c in columns)
                excluded = ','.join(f'EXCLUDED."{c}"' for c in columns)
                placeholders = ','.join(['%s'] * len(columns))
                upsert = f'''INSERT INTO stg.{table_name} AS t ({quoted}) VALUES ({placeholders})
                    ON CONFLICT ({key}) DO UPDATE SET {updates}
                    WHERE ROW({distinct}) IS DISTINCT FROM ROW({excluded})'''
                read_count = changed = 0
                while True:
                    batch = cur.fetchmany(5000)
                    if not batch:
                        break
                    read_count += len(batch)
                    target.executemany(upsert, batch)
                    changed += target.rowcount
                target.execute('''INSERT INTO etl.watermark(tabela,watermark) VALUES(%s,%s)
                    ON CONFLICT(tabela) DO UPDATE SET watermark=EXCLUDED.watermark''', (table_name, upper))
                target.execute('''INSERT INTO etl.extraction_log(run_id,tabela,limite_inferior,limite_superior,lidos,alterados)
                    VALUES(%s,%s,%s,%s,%s,%s)''', (context.get('run_id', 'execucao-direta'), table_name, lower, upper, read_count, changed))
                print(f'{table_name}: lidos={read_count}, novos/alterados={changed}, janela={lower}..{upper}')
    finally:
        source.close()
        pg.close()


def initialize_dw():
    for name in ('dw_d_produto.sql', 'dw_d_cliente.sql', 'dw_d_territorio.sql', 'dw_d_calendario.sql', 'dw_f_vendas.sql', 'migrate_precision.sql'):
        run_sql(name)
