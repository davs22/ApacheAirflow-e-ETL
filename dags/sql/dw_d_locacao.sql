-- dw: d_locacao
DROP TABLE IF EXISTS d_locacao CASCADE;
CREATE TABLE d_locacao (
    id SERIAL PRIMARY KEY,
    numero INT,
    tp_locacao VARCHAR(200)
);

