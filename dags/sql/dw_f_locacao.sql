-- dw: f_locacao
DROP TABLE IF EXISTS f_locacao CASCADE;
CREATE TABLE f_locacao (
    id SERIAL PRIMARY KEY,
    calendario_id INT REFERENCES d_calendario(id),
    cliente_id INT REFERENCES d_cliente(id),
    locacao_id INT REFERENCES d_locacao(id),
    anterior_dias INT,
    vlr_pgto FLOAT
);

