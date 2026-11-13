CREATE TABLE IF NOT EXISTS dw_f_vendas (
    sk_venda SERIAL PRIMARY KEY,
    
    sk_produto INT NOT NULL,
    sk_cliente INT NOT NULL,
    sk_territorio INT NOT NULL,
    sk_data_pedido INT NOT NULL,
    
    sales_order_id INT NOT NULL,
    sales_order_detail_id INT NOT NULL UNIQUE,
    
    quantidade INT NOT NULL,
    preco_unitario NUMERIC(19,4) NOT NULL,
    desconto NUMERIC(19,4) NOT NULL,
    custo_padrao NUMERIC(19,4) NOT NULL,
    valor_total NUMERIC(38,6) NOT NULL,
    
    data_modificacao TIMESTAMP NOT NULL, 
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (sk_produto) REFERENCES dw_d_produto(sk_produto),
    FOREIGN KEY (sk_cliente) REFERENCES dw_d_cliente(sk_cliente),
    FOREIGN KEY (sk_territorio) REFERENCES dw_d_territorio(sk_territorio),
    FOREIGN KEY (sk_data_pedido) REFERENCES dw_d_calendario(sk_data)
);
