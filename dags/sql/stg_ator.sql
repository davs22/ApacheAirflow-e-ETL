-- staging: ator
DROP TABLE IF EXISTS stg_ator;
CREATE TABLE stg_ator (
    cod INT,
    datanasc DATE,
    nacionalidade VARCHAR(10),
    nomereal VARCHAR(50),
    nomeartistico VARCHAR(50)
);

