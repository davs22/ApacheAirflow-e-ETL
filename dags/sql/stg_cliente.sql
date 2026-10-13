-- staging: cliente
DROP TABLE IF EXISTS stg_cliente;
CREATE TABLE stg_cliente (
    numcliente INT,
    nome VARCHAR(10),
    endereco VARCHAR(10),
    fonefixo VARCHAR(50),
    fonecel VARCHAR(10)
);

