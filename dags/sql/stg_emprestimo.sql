-- staging: emprestimo
DROP TABLE IF EXISTS stg_emprestimo;
CREATE TABLE stg_emprestimo (
    numfilme INT,
    numero INT,
    tipo VARCHAR(50),
    cliente INT,
    dataret DATE,
    datadev DATE,
    valor_pg FLOAT
);

