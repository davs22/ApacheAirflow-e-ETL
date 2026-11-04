DROP TABLE IF EXISTS dw_d_territorio CASCADE;

CREATE TABLE dw_d_territorio (
    sk_territorio SERIAL PRIMARY KEY,
    territory_id INT NOT NULL UNIQUE,
    nome_territorio VARCHAR(100) NOT NULL,
    codigo_pais VARCHAR(10) NOT NULL,
    regiao VARCHAR(100) NOT NULL,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);