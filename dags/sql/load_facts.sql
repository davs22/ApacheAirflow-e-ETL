WITH incoming (sales_order_detail_id, sk_produto, sk_cliente, sk_territorio, sk_data_pedido, sales_order_id, quantidade, preco_unitario, desconto, custo_padrao, valor_total, data_modificacao) AS (
SELECT sod.salesorderdetailid,dp.sk_produto,dc.sk_cliente,dt.sk_territorio,
TO_CHAR(soh.orderdate,'YYYYMMDD')::INT,soh.salesorderid,sod.orderqty,sod.unitprice,
sod.unitpricediscount,p.standardcost,sod.linetotal,GREATEST(sod.modifieddate,soh.modifieddate)
FROM stg.salesorderdetail sod JOIN stg.salesorderheader soh USING(salesorderid)
JOIN stg.product p USING(productid)
JOIN dw_d_produto dp ON dp.product_id=sod.productid
JOIN dw_d_cliente dc ON dc.customer_id=soh.customerid
JOIN dw_d_territorio dt ON dt.territory_id=COALESCE(soh.territoryid,-1)
)
INSERT INTO dw_f_vendas AS t (sales_order_detail_id, sk_produto, sk_cliente, sk_territorio, sk_data_pedido, sales_order_id, quantidade, preco_unitario, desconto, custo_padrao, valor_total, data_modificacao)
SELECT incoming.* FROM incoming LEFT JOIN dw_f_vendas existing ON existing.sales_order_detail_id=incoming.sales_order_detail_id
WHERE existing.sales_order_detail_id IS NULL OR ROW(existing.sk_produto,existing.sk_cliente,existing.sk_territorio,existing.sk_data_pedido,existing.sales_order_id,existing.quantidade,existing.preco_unitario,existing.desconto,existing.custo_padrao,existing.valor_total,existing.data_modificacao) IS DISTINCT FROM ROW(incoming.sk_produto,incoming.sk_cliente,incoming.sk_territorio,incoming.sk_data_pedido,incoming.sales_order_id,incoming.quantidade,incoming.preco_unitario,incoming.desconto,incoming.custo_padrao,incoming.valor_total,incoming.data_modificacao)
ON CONFLICT (sales_order_detail_id) DO UPDATE SET sk_produto=EXCLUDED.sk_produto, sk_cliente=EXCLUDED.sk_cliente, sk_territorio=EXCLUDED.sk_territorio, sk_data_pedido=EXCLUDED.sk_data_pedido, sales_order_id=EXCLUDED.sales_order_id, quantidade=EXCLUDED.quantidade, preco_unitario=EXCLUDED.preco_unitario, desconto=EXCLUDED.desconto, custo_padrao=EXCLUDED.custo_padrao, valor_total=EXCLUDED.valor_total, data_modificacao=EXCLUDED.data_modificacao, data_carga=CURRENT_TIMESTAMP
WHERE ROW(t.sk_produto, t.sk_cliente, t.sk_territorio, t.sk_data_pedido, t.sales_order_id, t.quantidade, t.preco_unitario, t.desconto, t.custo_padrao, t.valor_total, t.data_modificacao) IS DISTINCT FROM ROW(EXCLUDED.sk_produto, EXCLUDED.sk_cliente, EXCLUDED.sk_territorio, EXCLUDED.sk_data_pedido, EXCLUDED.sales_order_id, EXCLUDED.quantidade, EXCLUDED.preco_unitario, EXCLUDED.desconto, EXCLUDED.custo_padrao, EXCLUDED.valor_total, EXCLUDED.data_modificacao);
