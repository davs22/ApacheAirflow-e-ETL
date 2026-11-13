ALTER TABLE stg.product ALTER COLUMN standardcost TYPE NUMERIC(19,4);
ALTER TABLE stg.salesorderdetail ALTER COLUMN unitprice TYPE NUMERIC(19,4),
    ALTER COLUMN unitpricediscount TYPE NUMERIC(19,4), ALTER COLUMN linetotal TYPE NUMERIC(38,6);
ALTER TABLE dw_f_vendas ALTER COLUMN preco_unitario TYPE NUMERIC(19,4),
    ALTER COLUMN desconto TYPE NUMERIC(19,4), ALTER COLUMN custo_padrao TYPE NUMERIC(19,4),
    ALTER COLUMN valor_total TYPE NUMERIC(38,6);
CREATE INDEX IF NOT EXISTS ix_vendas_data ON dw_f_vendas(sk_data_pedido);
CREATE INDEX IF NOT EXISTS ix_vendas_produto ON dw_f_vendas(sk_produto);
CREATE INDEX IF NOT EXISTS ix_vendas_cliente ON dw_f_vendas(sk_cliente);
CREATE INDEX IF NOT EXISTS ix_vendas_territorio ON dw_f_vendas(sk_territorio);
