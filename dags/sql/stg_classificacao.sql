-- staging: classificacao
DROP TABLE IF EXISTS stg_classificacao;
CREATE TABLE stg_classificacao (
    cod INT,
    nome VARCHAR(50),
    preco FLOAT
);

