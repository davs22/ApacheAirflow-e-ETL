from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'diirr',
    # Cada execução pode rodar independentemente; uma execução anterior
    # com falha não deve bloquear as próximas.
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'etl_incremental_adventureworks',
    default_args=default_args,
    description='Carga Incremental do DW AdventureWorks',
    schedule='@daily',
    catchup=False,
    tags=['adventureworks', 'etl', 'incremental'],
) as dag:

    load_dim_produto = SQLExecuteQueryOperator(
        task_id='load_dim_produto',
        conn_id='postgres_default',
        sql="""
            INSERT INTO dw_d_produto (product_id, nome_produto, numero_produto, categoria, subcategoria)
            SELECT p.ProductID, p.Name, p.ProductNumber, c.Name, s.Name
            FROM stg.Product p
            LEFT JOIN stg.ProductSubcategory s ON p.ProductSubcategoryID = s.ProductSubcategoryID
            LEFT JOIN stg.ProductCategory c ON s.ProductCategoryID = c.ProductCategoryID
            WHERE p.ModifiedDate >= '{{ ds }}'
            ON CONFLICT (product_id) DO UPDATE 
            SET nome_produto = EXCLUDED.nome_produto,
                numero_produto = EXCLUDED.numero_produto,
                categoria = EXCLUDED.categoria,
                subcategoria = EXCLUDED.subcategoria,
                data_carga = CURRENT_TIMESTAMP;
        """
    )

    load_dim_territorio = SQLExecuteQueryOperator(
        task_id='load_dim_territorio',
        conn_id='postgres_default',
        sql="""
            INSERT INTO dw_d_territorio (territory_id, nome_territorio, codigo_pais, regiao)
            SELECT TerritoryID, Name, CountryRegionCode, "group"
            FROM stg.SalesTerritory
            WHERE ModifiedDate >= '{{ ds }}'
            ON CONFLICT (territory_id) DO UPDATE
            SET nome_territorio = EXCLUDED.nome_territorio,
                codigo_pais = EXCLUDED.codigo_pais,
                regiao = EXCLUDED.regiao,
                data_carga = CURRENT_TIMESTAMP;
        """
    )

    load_dim_cliente = SQLExecuteQueryOperator(
        task_id='load_dim_cliente',
        conn_id='postgres_default',
        sql="""
            INSERT INTO dw_d_cliente (customer_id, nome_cliente, tipo_cliente)
            SELECT CustomerID, 
                   COALESCE(PersonID::VARCHAR, 'Loja'), 
                   CASE WHEN PersonID IS NULL THEN 'Loja' ELSE 'Pessoa Física' END
            FROM stg.Customer
            WHERE ModifiedDate >= '{{ ds }}'
            ON CONFLICT (customer_id) DO UPDATE
            SET nome_cliente = EXCLUDED.nome_cliente,
                tipo_cliente = EXCLUDED.tipo_cliente,
                data_carga = CURRENT_TIMESTAMP;
        """
    )

    load_fato_vendas = SQLExecuteQueryOperator(
        task_id='load_fato_vendas',
        conn_id='postgres_default',
        sql="""
            INSERT INTO dw_f_vendas (sk_produto, sk_cliente, sk_territorio, sk_data_pedido, sales_order_id, sales_order_detail_id, quantidade, preco_unitario, desconto, custo_padrao, valor_total, data_modificacao)
            SELECT 
                dp.sk_produto,
                dc.sk_cliente,
                dt.sk_territorio,
                TO_CHAR(soh.OrderDate, 'YYYYMMDD')::INT,
                soh.SalesOrderID,
                sod.SalesOrderDetailID,
                sod.OrderQty,
                sod.UnitPrice,
                sod.UnitPriceDiscount,
                COALESCE(p.StandardCost, 0),
                sod.LineTotal,
                GREATEST(sod.ModifiedDate, soh.ModifiedDate)
            FROM stg.SalesOrderDetail sod
            INNER JOIN stg.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
            INNER JOIN stg.Product p ON sod.ProductID = p.ProductID
            INNER JOIN dw_d_produto dp ON dp.product_id = sod.ProductID
            INNER JOIN dw_d_cliente dc ON dc.customer_id = soh.CustomerID
            INNER JOIN dw_d_territorio dt ON dt.territory_id = soh.TerritoryID
            WHERE sod.ModifiedDate >= '{{ ds }}'
               OR soh.ModifiedDate >= '{{ ds }}'
            ON CONFLICT (sales_order_detail_id) DO UPDATE
            SET sk_produto = EXCLUDED.sk_produto,
                sk_cliente = EXCLUDED.sk_cliente,
                sk_territorio = EXCLUDED.sk_territorio,
                sk_data_pedido = EXCLUDED.sk_data_pedido,
                sales_order_id = EXCLUDED.sales_order_id,
                quantidade = EXCLUDED.quantidade,
                preco_unitario = EXCLUDED.preco_unitario,
                desconto = EXCLUDED.desconto,
                custo_padrao = EXCLUDED.custo_padrao,
                valor_total = EXCLUDED.valor_total,
                data_modificacao = EXCLUDED.data_modificacao,
                data_carga = CURRENT_TIMESTAMP;
        """
    )

    [load_dim_produto, load_dim_territorio, load_dim_cliente] >> load_fato_vendas
