SELECT SUM(valor_total) AS Faturamento_Total 
FROM dw_f_vendas;

SELECT SUM(quantidade * custo_padrao) AS Custo_Total 
FROM dw_f_vendas;

SELECT SUM(valor_total) - SUM(quantidade * custo_padrao) AS Lucro_Bruto 
FROM dw_f_vendas;

SELECT 
    ROUND(((SUM(valor_total) - SUM(quantidade * custo_padrao)) / SUM(valor_total)) * 100, 2) AS Margem_Lucro_Perc 
FROM dw_f_vendas;

SELECT COUNT(DISTINCT sales_order_id) AS Total_Pedidos 
FROM dw_f_vendas;

SELECT 
    SUM(valor_total) / COUNT(DISTINCT sales_order_id) AS Ticket_Medio 
FROM dw_f_vendas;

SELECT SUM(quantidade) AS Total_Unidades_Vendidas 
FROM dw_f_vendas;

SELECT 
    dp.categoria, 
    SUM(fv.valor_total) AS Faturamento 
FROM dw_f_vendas fv
INNER JOIN dw_d_produto dp ON fv.sk_produto = dp.sk_produto
GROUP BY dp.categoria
ORDER BY Faturamento DESC;

SELECT 
    dt.regiao, 
    dt.nome_territorio, 
    SUM(fv.valor_total) AS Faturamento 
FROM dw_f_vendas fv
INNER JOIN dw_d_territorio dt ON fv.sk_territorio = dt.sk_territorio
GROUP BY dt.regiao, dt.nome_territorio
ORDER BY Faturamento DESC;

SELECT 
    dp.nome_produto, 
    SUM(fv.valor_total) - SUM(fv.quantidade * fv.custo_padrao) AS Lucro_Gerado 
FROM dw_f_vendas fv
INNER JOIN dw_d_produto dp ON fv.sk_produto = dp.sk_produto
GROUP BY dp.nome_produto
ORDER BY Lucro_Gerado DESC
LIMIT 10;
