import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

config_tabelas = {
    'productcategory': {
        'file': 'ProductCategory.csv',
        'cols': [0, 1, 3], 
        'names': ['productcategoryid', 'name', 'modifieddate']
    },
    'productsubcategory': {
        'file': 'ProductSubcategory.csv',
        'cols': [0, 1, 2, 4],
        'names': ['productsubcategoryid', 'productcategoryid', 'name', 'modifieddate']
    },
    'product': {
        'file': 'Product.csv',
        'cols': [0, 1, 2, 8, 18, 24],
        'names': ['productid', 'name', 'productnumber', 'standardcost', 'productsubcategoryid', 'modifieddate']
    },
    'salesterritory': {
        'file': 'SalesTerritory.csv',
        'cols': [0, 1, 2, 3, 9],
        'names': ['territoryid', 'name', 'countryregioncode', 'group', 'modifieddate']
    },
    'customer': {
        'file': 'Customer.csv',
        'cols': [0, 1, 2, 3, 6],
        'names': ['customerid', 'personid', 'storeid', 'territoryid', 'modifieddate']
    },
    'salesorderheader': {
        'file': 'SalesOrderHeader.csv',
        'cols': [0, 2, 10, 12, 25],
        'names': ['salesorderid', 'orderdate', 'customerid', 'territoryid', 'modifieddate']
    },
    'salesorderdetail': {
        'file': 'SalesOrderDetail.csv',
        'cols': [0, 1, 3, 4, 6, 7, 8, 10],
        'names': ['salesorderid', 'salesorderdetailid', 'orderqty', 'productid', 'unitprice', 'unitpricediscount', 'linetotal', 'modifieddate']
    }
}

def carregar_dados_stg(table_name, **kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    conf = config_tabelas[table_name]
    file_path = f"/opt/airflow/dags/data/{conf['file']}"

    print(f"Lendo arquivo: {file_path}")
    df = pd.read_csv(file_path, sep='\t', header=None, usecols=conf['cols'], names=conf['names'])

    for column in df.columns:
        if column.endswith('id') or column == 'orderqty':
            df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')

    with engine.begin() as conn:
        for column in df.columns:
            if column in {'modifieddate', 'orderdate'}:
                df[column] = pd.to_datetime(df[column], errors='coerce')

        df = df.astype(object).where(pd.notnull(df), None)
        conn.execute(text(f"TRUNCATE TABLE stg.{table_name};"))
        df.to_sql(
            table_name,
            conn,
            schema='stg',
            if_exists='append',
            index=False,
            chunksize=5000,
        )

    print(f"Sucesso! {len(df)} linhas carregadas na tabela stg.{table_name}")

with DAG(
    'etl_extract_stg_adventureworks_real',
    start_date=datetime(2019, 1, 1),
    schedule=None,
    catchup=False,
    tags=['adventureworks', 'stg', 'extract', 'real_data']
) as dag:

    for tabela in config_tabelas.keys():
        PythonOperator(
            task_id=f'extract_{tabela}',
            python_callable=carregar_dados_stg,
            op_kwargs={'table_name': tabela}
        )
