-- dw: d_calendario
DROP TABLE IF EXISTS d_calendario CASCADE;
CREATE TABLE d_calendario (
    id SERIAL PRIMARY KEY,
    data DATE,
    dia INT,
    mes INT,
    ano INT,
    mes_ano INT,
    nome_mes VARCHAR(50),
    nome_dia_semana VARCHAR(50),
    dia_semana INT,
    eh_feriado BOOLEAN
);

