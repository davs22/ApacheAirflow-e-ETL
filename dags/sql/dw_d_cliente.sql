-- dw: d_cliente
DROP TABLE IF EXISTS d_cliente CASCADE;
CREATE TABLE d_cliente (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(256)
);

