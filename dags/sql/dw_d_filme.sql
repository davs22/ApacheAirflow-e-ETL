-- dw: d_filme
DROP TABLE IF EXISTS d_filme CASCADE;
CREATE TABLE d_filme (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(300),
    categoria VARCHAR(256)
);

