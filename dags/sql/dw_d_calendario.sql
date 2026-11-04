CREATE TABLE IF NOT EXISTS dw_d_calendario (
    sk_data INT PRIMARY KEY, 
    data_completa DATE NOT NULL,
    ano INT NOT NULL,
    mes INT NOT NULL,
    nome_mes VARCHAR(50) NOT NULL,
    trimestre INT NOT NULL,
    dia INT NOT NULL,
    nome_dia_semana VARCHAR(50) NOT NULL
);

INSERT INTO dw_d_calendario (sk_data, data_completa, ano, mes, nome_mes, trimestre, dia, nome_dia_semana)
SELECT
    TO_CHAR(d.data_completa, 'YYYYMMDD')::INT,
    d.data_completa,
    EXTRACT(YEAR FROM d.data_completa)::INT,
    EXTRACT(MONTH FROM d.data_completa)::INT,
    TO_CHAR(d.data_completa, 'TMMonth'),
    EXTRACT(QUARTER FROM d.data_completa)::INT,
    EXTRACT(DAY FROM d.data_completa)::INT,
    TO_CHAR(d.data_completa, 'TMDay')
FROM GENERATE_SERIES(
    (SELECT MIN(orderdate)::DATE FROM stg.salesorderheader),
    (SELECT MAX(orderdate)::DATE FROM stg.salesorderheader),
    INTERVAL '1 day'
) AS d(data_completa)
ON CONFLICT (sk_data) DO NOTHING;
