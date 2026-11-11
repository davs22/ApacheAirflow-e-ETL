import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'diirr',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Mapeamento das consultas diretamente nos schemas originais do SQL Server (AdventureWorks OLTP)
# Utilizamos aliases em minúsculo (AS productcategoryid) para garantir compatibilidade perfeita com o PostgreSQL
QUERIES_EXTRACAO = {
    'productcategory': """
        SELECT 
            ProductCategoryID AS productcategoryid, 
            Name AS name, 
            ModifiedDate AS modifieddate
        FROM Production.ProductCategory;
    """,
    'productsubcategory': """
        SELECT 
            ProductSubcategoryID AS productsubcategoryid, 
            ProductCategoryID AS productcategoryid, 
            Name AS name, 
            ModifiedDate AS modifieddate
        FROM Production.ProductSubcategory;
    """,
    'product': """
        SELECT 
            ProductID AS productid, 
            Name AS name, 
            ProductNumber AS productnumber, 
            ProductSubcategoryID AS productsubcategoryid, 
            StandardCost AS standardcost, 
            ModifiedDate AS modifieddate
        FROM Production.Product;
    """,
    'salesterritory': """
        SELECT 
            TerritoryID AS territoryid, 
            Name AS name, 
            CountryRegionCode AS countryregioncode, 
            [Group] AS "group", 
            ModifiedDate AS modifieddate
        FROM Sales.SalesTerritory;
    """,
    'customer': """
        SELECT 
            CustomerID AS customerid, 
            PersonID AS personid, 
            StoreID AS storeid, 
            TerritoryID AS territoryid, 
            ModifiedDate AS modifieddate
        FROM Sales.Customer;
    """,
    'salesorderheader': """
        SELECT 
            SalesOrderID AS salesorderid, 
            OrderDate AS orderdate, 
            CustomerID AS customerid, 
            TerritoryID AS territoryid, 
            ModifiedDate AS modifieddate
        FROM Sales.SalesOrderHeader;
    """,
    'salesorderdetail': """
        SELECT 
            SalesOrderID AS salesorderid, 
            SalesOrderDetailID AS salesorderdetailid, 
            OrderQty AS orderqty, 
            ProductID AS productid, 
            UnitPrice AS unitprice, 
            UnitPriceDiscount AS unitpricediscount, 
            LineTotal AS linetotal, 
            ModifiedDate AS modifieddate
        FROM Sales.SalesOrderDetail;
    """
}

def extrair_sqlserver_para_postgres(table_name, sql_query, **kwargs):
    # 1. Conecta na origem (SQL Server - AdventureWorks) usando a conexão do painel
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_adventureworks')
    df = mssql_hook.get_pandas_df(sql=sql_query)
    
    # Tratamento de tipos nulos do Pandas para não gerar conflito no PostgreSQL
    df = df.astype(object).where(pd.notnull(df), None)
    
    # 2. Conecta no destino (PostgreSQL - Staging) usando a conexão do painel
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_engine = pg_hook.get_sqlalchemy_engine()
    
    with pg_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE stg.{table_name} CASCADE;"))
        
    df.to_sql(table_name, pg_engine, schema='stg', if_exists='append', index=False, chunksize=5000)
    print(f"Tabela stg.{table_name} carregada com sucesso! Total de registros extraídos: {len(df)}")

with DAG(
    'etl_extract_mssql_adventureworks',
    default_args=default_args,
    description='Extração tradicional OLTP do SQL Server para Staging no PostgreSQL',
    schedule=None,
    catchup=False,
    tags=['adventureworks', 'mssql', 'postgres', 'extract', 'stg'],
) as dag:

    # Cria as tarefas paralelamente para otimizar o tempo de extração
    for tabela, query in QUERIES_EXTRACAO.items():
        PythonOperator(
            task_id=f'extract_{tabela}',
            python_callable=extrair_sqlserver_para_postgres,
            op_kwargs={
                'table_name': tabela,
                'sql_query': query
            }
        )