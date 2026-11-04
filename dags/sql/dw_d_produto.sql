DROP TABLE IF EXISTS dw_d_produto CASCADE;

CREATE TABLE dw_d_produto (
    sk_produto SERIAL PRIMARY KEY,
    product_id INT NOT NULL, -- Chave de negócio (Natural Key)
    nome_produto VARCHAR(255) NOT NULL,
    numero_produto VARCHAR(50),
    categoria VARCHAR(100) DEFAULT 'Sem Categoria',
    subcategoria VARCHAR(100) DEFAULT 'Sem Subcategoria',
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);