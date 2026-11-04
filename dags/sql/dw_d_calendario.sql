DROP TABLE IF EXISTS dw_d_calendario CASCADE;

CREATE TABLE dw_d_calendario (
    sk_data INT PRIMARY KEY, -- Formato YYYYMMDD (ex: 20261103)
    data_completa DATE NOT NULL,
    ano INT NOT NULL,
    mes INT NOT NULL,
    nome_mes VARCHAR(50) NOT NULL,
    trimestre INT NOT NULL,
    dia INT NOT NULL,
    nome_dia_semana VARCHAR(50) NOT NULL
);