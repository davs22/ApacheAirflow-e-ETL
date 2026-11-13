# Documentação técnica do Data Warehouse AdventureWorks

## Domínio e análise do OLTP

O recorte é o processo de vendas do AdventureWorks2019. O SQL Server mantém um
modelo normalizado: Sales.SalesOrderHeader representa o pedido e
Sales.SalesOrderDetail seus itens (1:N). Cada item referencia Production.Product;
o produto aponta para ProductSubcategory e esta para ProductCategory. O cabeçalho
referencia Sales.Customer e Sales.SalesTerritory. Customer pode representar uma
pessoa, com dados em Person.Person, ou uma loja em Sales.Store.

Somar valores de cabeçalho após juntá-los aos itens multiplicaria o faturamento.
Por isso a medida de receita parte de SalesOrderDetail.LineTotal. Pedidos são
contados com DISTINCT SalesOrderID. A extração consulta nove tabelas da origem
(as duas tabelas de nomes são juntadas na consulta de clientes) e mantém sete
tabelas na staging PostgreSQL.

## Modelo estrela, grão e decisões

O grão é **um item de pedido**, identificado por SalesOrderDetailID, único no
AdventureWorks. SalesOrderID é uma dimensão degenerada na fato: permite agrupar
itens e calcular ticket sem uma dimensão artificial de pedidos.

Quatro dimensões desnormalizadas ligam-se diretamente à fato. Categoria e
subcategoria ficam em dw_d_produto, sem dimensões auxiliares, preservando o
padrão Star Schema. As dimensões de produto, cliente e território usam chaves
substitutas SERIAL e chaves naturais UNIQUE. O calendário usa YYYYMMDD como PK.
As quatro FKs da fato são obrigatórias e verificadas pelo PostgreSQL.

As dimensões seguem SCD tipo 1: atributos corrigidos substituem os anteriores;
o modelo não preserva histórico de classificação do cliente ou categoria.
Território nulo mapeia para um membro explícito de chave natural -1. Produtos
sem categoria/subcategoria recebem rótulos explícitos. O calendário é expandido
antes da fato em cada execução, usando o intervalo de pedidos da staging.

O diagrama editável está em [modelo_estrela.mmd](modelo_estrela.mmd), e a versão
visual em [modelo_estrela.svg](modelo_estrela.svg). O desenho preexistente em
data/Star Schema.png foi preservado.

## Dicionário de dados

Os scripts dags/sql/dw_d_*.sql e dw_f_vendas.sql definem tipos e restrições.

- **dw_d_produto:** sk_produto (SERIAL, PK); product_id (INT, UNIQUE NOT NULL,
  chave natural); nome_produto (VARCHAR 255, obrigatório); numero_produto
  (VARCHAR 50); categoria e subcategoria (VARCHAR 100); data_carga (TIMESTAMP).
- **dw_d_cliente:** sk_cliente (SERIAL, PK); customer_id (INT, UNIQUE NOT NULL);
  nome_cliente (VARCHAR 255, nome da loja ou pessoa); tipo_cliente (VARCHAR 50,
  Loja/Pessoa Física); data_carga (TIMESTAMP).
- **dw_d_territorio:** sk_territorio (SERIAL, PK); territory_id (INT, UNIQUE NOT
  NULL); nome_territorio (VARCHAR 100), codigo_pais (VARCHAR 10), regiao (VARCHAR
  100), todos obrigatórios; data_carga (TIMESTAMP).
- **dw_d_calendario:** sk_data (INT, PK); data_completa (DATE); ano, mes,
  trimestre, dia (INT); nome_mes e nome_dia_semana (VARCHAR 50). Todos
  obrigatórios. Os nomes são produzidos pelo locale do PostgreSQL.
- **dw_f_vendas:** sk_venda (SERIAL, PK); sales_order_detail_id (INT, UNIQUE),
  sales_order_id (INT); sk_produto, sk_cliente, sk_territorio e sk_data_pedido
  (INT, FKs); quantidade (INT); preco_unitario, desconto e custo_padrao
  (NUMERIC 19,4); valor_total (NUMERIC 38,6); data_modificacao e data_carga
  (TIMESTAMP). Todos são NOT NULL exceto data_carga, que possui DEFAULT.
  desconto é uma proporção (0 a 1), não o desconto monetário.
- **etl.watermark:** tabela (TEXT, PK) e watermark (TIMESTAMP NOT NULL), limite
  superior da última extração confirmada.
- **etl.extraction_log:** id (BIGSERIAL, PK), run_id e tabela (TEXT), limites
  inferior e superior (TIMESTAMP), lidos e alterados (INT), criado_em
  (TIMESTAMPTZ). O limite inferior é NULL no bootstrap.

## Fluxo Airflow e estratégia incremental

DAG principal: etl_incremental_adventureworks, diário (@daily, UTC), catchup=False,
max_active_runs=1, uma tentativa adicional após um minuto. Ele tem 13 tarefas:

1. create_staging cria staging e controle sem DROP/TRUNCATE.
2. create_dw cria as quatro dimensões e a fato, aplica migração de precisão e índices.
3. Sete extract_* leem as sete consultas da origem e alimentam staging.
4. load_dimensions atualiza apenas dimensões novas ou de conteúdo diferente e calendário.
5. load_facts insere/atualiza apenas itens novos ou diferentes, preservando sk_venda.
6. validate_dw exige staging não vazia, contagem igual, receita igual, medidas
   individuais iguais e fórmula quantidade × preço × (1-desconto) consistente.
7. calculate_10_kpis executa as dez consultas e registra resultados no log.

As sete extrações dependem da criação do DW. Todas precisam terminar para a
carga das dimensões, seguida da fato, da qualidade e dos KPIs. Em caso de erro,
as tarefas dependentes não prosseguem. Os DAGs de criação e extração separados
são auxiliares manuais (schedule=None); não são pré-requisitos manuais do principal.
Evite dispará-los em paralelo ao DAG principal durante manutenção.

No bootstrap, não existe watermark: todo o histórico até GETDATE() da origem é
extraído. Depois, o filtro na própria origem é ModifiedDate entre watermark
anterior menos um dia e GETDATE() atual. A sobreposição é intencional para
empates de timestamp e pequenas transações em andamento. Linhas iguais podem
ser relidas nessa janela, mas não são atualizadas: IS DISTINCT FROM compara
todo o conteúdo com segurança para NULL. Clientes usam o maior ModifiedDate
entre Customer, Person e Store para capturar mudanças apenas no nome.

Cada tabela tem bloqueio transacional consultivo. A staging, o watermark e o
log de extração são confirmados na mesma transação PostgreSQL. Falha reverte os
três. Uma falha posterior na carga do DW pode ser retomada porque a staging
persiste e as cargas comparam conteúdo, em vez de filtrar por data de execução.
Mudanças apenas no cabeçalho, categoria ou subcategoria também são propagadas.
Os joins comparam o conjunto da staging, mas só as linhas novas/diferentes são
enviadas ao INSERT/UPDATE do DW.

**Contrato e limites:** ModifiedDate precisa avançar nas alterações da origem;
transações atrasadas em mais de um dia e correções retrodatadas fora da janela
exigem reextração da tabela. Exclusões físicas não são capturadas; o requisito
do professor pede dados novos/modificados, sem exigir CDC de exclusões. Não há
snapshot distribuído entre as sete consultas: alterações OLTP concorrentes
podem exigir uma nova execução para reconciliar dependências. Para recuperar
dados fora da janela, apague somente o watermark da tabela afetada e execute o
pipeline novamente; não é necessário apagar staging ou fato.

## Dez indicadores e interpretação

Implementação: dags/sql/kpis_adventureworks.sql, com uma consulta por indicador.

1. Faturamento líquido = SUM(valor_total), após desconto, sem frete e impostos.
2. Custo estimado = SUM(quantidade × custo_padrao).
3. Lucro bruto estimado = faturamento - custo estimado.
4. Margem bruta estimada (%) = 100 × lucro / faturamento.
5. Total de pedidos = COUNT(DISTINCT sales_order_id).
6. Ticket médio = faturamento / total de pedidos.
7. Unidades vendidas = SUM(quantidade).
8. Faturamento por categoria, agregado pela dimensão produto.
9. Faturamento por região e território, agregado pela dimensão território.
10. Top 10 produtos por lucro bruto estimado, agrupados por ID e nome.

NULLIF evita divisão por zero; margem e ticket são NULL quando indefinidos.
As somas retornam zero para fato vazia. Valores são mantidos na moeda da amostra,
sem assumir BRL ou introduzir uma conversão ausente na origem.

StandardCost é o custo **atual** do produto; não é o custo histórico por venda.
Portanto lucro, margem e ranking de lucro são estimativas baseadas nesse custo,
e mudanças de custo atualizam os itens associados. Uma margem contábil histórica
exigiria ProductCostHistory e uma regra temporal, fora do recorte adotado.

## Verificação e evidências

scripts/run_validation.py reconcilia a base real sem inserir dados de teste.
scripts/test_incremental_isolated.py cria bancos únicos com prefixo aw_audit_,
clona as tabelas necessárias, testa alterações e os remove em finally. Nenhuma
dessas alterações de teste é feita no AdventureWorks original.
scripts/export_evidence.py exporta contagens, KPIs e estados dos DAGs sem senhas.
Os resultados auditados estão em docs/evidencias e no relatório de conformidade.

O repositório remoto configurado é https://github.com/davs22/ApacheAirflow-e-ETL.
Senhas locais ficam em .env ignorado pelo Git. O backup anterior à auditoria está
em data/backup/antes_auditoria.dump, também ignorado. Não publique esses arquivos.

Referências técnicas consultadas: [operadores Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/operators-and-hooks-ref.html),
[INSERT/ON CONFLICT PostgreSQL](https://www.postgresql.org/docs/16/sql-insert.html).
