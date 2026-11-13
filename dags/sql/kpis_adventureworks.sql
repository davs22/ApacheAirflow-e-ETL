-- Dez indicadores. Receita líquida de desconto, sem frete/impostos.
-- 1. Faturamento líquido (moeda do AdventureWorks).
SELECT COALESCE(SUM(valor_total),0) faturamento_total FROM dw_f_vendas;
-- 2. Custo estimado: quantidade x StandardCost atual do produto.
SELECT COALESCE(SUM(quantidade*custo_padrao),0) custo_total_estimado FROM dw_f_vendas;
-- 3. Lucro bruto estimado.
SELECT COALESCE(SUM(valor_total-quantidade*custo_padrao),0) lucro_bruto_estimado FROM dw_f_vendas;
-- 4. Margem bruta estimada; NULL quando não há receita.
SELECT ROUND(100*SUM(valor_total-quantidade*custo_padrao)/NULLIF(SUM(valor_total),0),2) margem_lucro_perc FROM dw_f_vendas;
-- 5. Pedidos distintos.
SELECT COUNT(DISTINCT sales_order_id) total_pedidos FROM dw_f_vendas;
-- 6. Ticket médio por pedido (não por item).
SELECT SUM(valor_total)/NULLIF(COUNT(DISTINCT sales_order_id),0) ticket_medio FROM dw_f_vendas;
-- 7. Unidades vendidas.
SELECT COALESCE(SUM(quantidade),0) unidades_vendidas FROM dw_f_vendas;
-- 8. Faturamento por categoria.
SELECT p.categoria,SUM(f.valor_total) faturamento FROM dw_f_vendas f
JOIN dw_d_produto p USING(sk_produto) GROUP BY p.categoria ORDER BY faturamento DESC,p.categoria;
-- 9. Faturamento por região e território.
SELECT t.regiao,t.nome_territorio,SUM(f.valor_total) faturamento FROM dw_f_vendas f
JOIN dw_d_territorio t USING(sk_territorio) GROUP BY t.regiao,t.nome_territorio
ORDER BY faturamento DESC,t.nome_territorio;
-- 10. Dez produtos de maior lucro bruto estimado.
SELECT p.product_id,p.nome_produto,SUM(f.valor_total-f.quantidade*f.custo_padrao) lucro_gerado
FROM dw_f_vendas f JOIN dw_d_produto p USING(sk_produto)
GROUP BY p.product_id,p.nome_produto ORDER BY lucro_gerado DESC,p.product_id LIMIT 10;
