-- Dimensões SCD tipo 1. Comparação de conteúdo evita atualizações sem mudança.
-- Reavaliação dos joins propaga mudanças de categoria/subcategoria sem mudar Product.ModifiedDate.
WITH incoming (product_id, nome_produto, numero_produto, categoria, subcategoria) AS (
SELECT p.productid,p.name,p.productnumber,COALESCE(c.name,'Sem Categoria'),COALESCE(s.name,'Sem Subcategoria')
FROM stg.product p LEFT JOIN stg.productsubcategory s USING(productsubcategoryid)
LEFT JOIN stg.productcategory c USING(productcategoryid)
)
INSERT INTO dw_d_produto AS t (product_id, nome_produto, numero_produto, categoria, subcategoria)
SELECT incoming.* FROM incoming LEFT JOIN dw_d_produto existing ON existing.product_id=incoming.product_id
WHERE existing.product_id IS NULL OR ROW(existing.nome_produto,existing.numero_produto,existing.categoria,existing.subcategoria) IS DISTINCT FROM ROW(incoming.nome_produto,incoming.numero_produto,incoming.categoria,incoming.subcategoria)
ON CONFLICT (product_id) DO UPDATE SET nome_produto=EXCLUDED.nome_produto, numero_produto=EXCLUDED.numero_produto, categoria=EXCLUDED.categoria, subcategoria=EXCLUDED.subcategoria, data_carga=CURRENT_TIMESTAMP
WHERE ROW(t.nome_produto, t.numero_produto, t.categoria, t.subcategoria) IS DISTINCT FROM ROW(EXCLUDED.nome_produto, EXCLUDED.numero_produto, EXCLUDED.categoria, EXCLUDED.subcategoria);

WITH incoming (customer_id, nome_cliente, tipo_cliente) AS (
SELECT customerid,COALESCE(nome_cliente,'Cliente sem nome'),CASE WHEN storeid IS NOT NULL THEN 'Loja' ELSE 'Pessoa Física' END FROM stg.customer
)
INSERT INTO dw_d_cliente AS t (customer_id, nome_cliente, tipo_cliente)
SELECT incoming.* FROM incoming LEFT JOIN dw_d_cliente existing ON existing.customer_id=incoming.customer_id
WHERE existing.customer_id IS NULL OR ROW(existing.nome_cliente,existing.tipo_cliente) IS DISTINCT FROM ROW(incoming.nome_cliente,incoming.tipo_cliente)
ON CONFLICT (customer_id) DO UPDATE SET nome_cliente=EXCLUDED.nome_cliente, tipo_cliente=EXCLUDED.tipo_cliente, data_carga=CURRENT_TIMESTAMP
WHERE ROW(t.nome_cliente, t.tipo_cliente) IS DISTINCT FROM ROW(EXCLUDED.nome_cliente, EXCLUDED.tipo_cliente);

INSERT INTO dw_d_territorio(territory_id,nome_territorio,codigo_pais,regiao) VALUES(-1,'Não informado','N/A','Não informado') ON CONFLICT(territory_id) DO NOTHING;
WITH incoming (territory_id, nome_territorio, codigo_pais, regiao) AS (
SELECT territoryid,name,countryregioncode,"group" FROM stg.salesterritory
)
INSERT INTO dw_d_territorio AS t (territory_id, nome_territorio, codigo_pais, regiao)
SELECT incoming.* FROM incoming LEFT JOIN dw_d_territorio existing ON existing.territory_id=incoming.territory_id
WHERE existing.territory_id IS NULL OR ROW(existing.nome_territorio,existing.codigo_pais,existing.regiao) IS DISTINCT FROM ROW(incoming.nome_territorio,incoming.codigo_pais,incoming.regiao)
ON CONFLICT (territory_id) DO UPDATE SET nome_territorio=EXCLUDED.nome_territorio, codigo_pais=EXCLUDED.codigo_pais, regiao=EXCLUDED.regiao, data_carga=CURRENT_TIMESTAMP
WHERE ROW(t.nome_territorio, t.codigo_pais, t.regiao) IS DISTINCT FROM ROW(EXCLUDED.nome_territorio, EXCLUDED.codigo_pais, EXCLUDED.regiao);


CREATE TABLE IF NOT EXISTS dw_d_calendario (
    sk_data INT PRIMARY KEY,
    data_completa DATE NOT NULL,
    ano INT NOT NULL,
    mes INT NOT NULL,
    nome_mes VARCHAR(50) NOT NULL,
    trimestre INT NOT NULL,
    dia INT NOT NULL,
    nome_dia_semana VARCHAR(50) NOT NULL
);

INSERT INTO dw_d_calendario (sk_data, data_completa, ano, mes, nome_mes, trimestre, dia, nome_dia_semana)
SELECT
    TO_CHAR(d.data_completa, 'YYYYMMDD')::INT,
    d.data_completa,
    EXTRACT(YEAR FROM d.data_completa)::INT,
    EXTRACT(MONTH FROM d.data_completa)::INT,
    TO_CHAR(d.data_completa, 'TMMonth'),
    EXTRACT(QUARTER FROM d.data_completa)::INT,
    EXTRACT(DAY FROM d.data_completa)::INT,
    TO_CHAR(d.data_completa, 'TMDay')
FROM GENERATE_SERIES(
    (SELECT MIN(orderdate)::DATE FROM stg.salesorderheader),
    (SELECT MAX(orderdate)::DATE FROM stg.salesorderheader),
    INTERVAL '1 day'
) AS d(data_completa)
ON CONFLICT (sk_data) DO NOTHING;
