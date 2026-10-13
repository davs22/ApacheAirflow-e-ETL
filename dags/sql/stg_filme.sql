-- staging: filme
DROP TABLE IF EXISTS stg_filme;
CREATE TABLE stg_filme (
    numfilme INT,
    titulo_original VARCHAR(50),
    titulo_pt VARCHAR(50),
    duracao INT,
    data_lancamento DATE,
    direcao VARCHAR(256),
    categoria VARCHAR(50),
    classificacao INT
);

