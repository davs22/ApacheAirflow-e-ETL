CREATE TABLE IF NOT EXISTS dw_d_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    customer_id INT NOT NULL UNIQUE,
    nome_cliente VARCHAR(255),
    tipo_cliente VARCHAR(50), -- Loja ou Pessoa Física
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
