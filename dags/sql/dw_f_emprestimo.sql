-- dw: f_emprestimo
DROP TABLE IF EXISTS f_emprestimo CASCADE;
CREATE TABLE f_emprestimo (
    id SERIAL PRIMARY KEY,
    calendario_id INT REFERENCES d_calendario(id),
    cliente_id INT REFERENCES d_cliente(id),
    filme_id INT REFERENCES d_filme(id),
    locacao_id INT REFERENCES d_locacao(id),
    midia_id INT REFERENCES d_midia(id),
    vlr_pgto FLOAT
);

