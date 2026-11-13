# Relatório de conformidade e correções

Data: 13/11/2026, America/Sao_Paulo.

## Escopo e conclusão

Foram lidas visualmente as quatro páginas do PDF fornecido pelo usuário,
inspecionados todos os DAGs, todos os SQLs e a configuração Docker, e executados
os componentes reais do projeto. O artigo acadêmico, sua formatação/limite de
páginas e os prints de execução no Airflow foram excluídos por solicitação
expressa do usuário. Os conteúdos técnicos úteis ao projeto foram documentados
em documentação própria, sem produzir um artigo. As listas de alunos não foram
usadas nem reproduzidas.

**O estado inicial não atendia integralmente aos requisitos técnicos.** A
existência de dados e execuções verdes não comprovava uma extração incremental
correta nem inicialização reproduzível. As lacunas técnicas identificadas foram
corrigidas e verificadas: 15 testes automatizados aprovados, além das 13 tarefas
do DAG principal executadas com sucesso no Airflow real. A receita e a quantidade
de itens foram reconciliadas diretamente com o SQL Server, sem divergência.

O parecer se refere ao recorte de vendas e ao contrato incremental documentado.
Não representa garantia de nota ou de domínio individual do aluno. A existência
do GitHub foi confirmada pela API; a versão corrigida está organizada na branch
`codex/conformidade-professor-2026-11-13`, incluindo os novos arquivos desta auditoria.

## Requisitos conferidos

1. **Analisar o AdventureWorks OLTP — atendido após documentação.** Foram
   descritas as relações pedido/item/produto/categoria/cliente/território e as
   duas fontes de nomes de clientes. Evidência: documentacao_tecnica.md.
2. **Definir dez indicadores relevantes — atendido.** Já havia dez consultas;
   foram acrescentadas definições, unidades e ressalvas sobre custos estimados.
   Evidência: documentação e kpis_adventureworks.sql.
3. **Star Schema obrigatório — atendido.** Quatro dimensões diretamente ligadas
   à fato; categoria/subcategoria desnormalizadas. PKs, UKs e FKs verificadas.
4. **Diagrama do modelo — atendido.** Havia imagem local não versionada;
   acrescentadas fonte Mermaid e versão SVG coerentes com os scripts finais.
5. **DW no PostgreSQL — atendido e executado.** Tabelas criadas/migradas,
   dimensões populadas e 121.317 fatos reconciliados.
6. **ETL no Apache Airflow — atendido e executado.** Fluxo principal completo,
   com 13 tarefas, agendamento diário e dependências explícitas.
7. **Carga incremental obrigatória — corrigida e testada.** Watermark durável
   por consulta/tabela, filtro na origem, janela sobreposta e gravação apenas de
   conteúdos novos/diferentes. Bootstrap, atualização e retomada testados.
8. **Indicadores implementados em SQL — atendido e executado.** Dez consultas
   na base preenchida e também na fato vazia, sem divisão por zero.
9. **Repositório GitHub com DAGs, DDL, KPIs e documentação — repositório
   existente confirmado.** A documentação ausente foi criada localmente junto
   das correções. O conjunto a entregar inclui dags/, scripts/, docs/ e README.
10. **Reconhecimento de padrões — evidenciado.** A análise de resultados abaixo
    identifica concentração de receita e produtos de maior lucro estimado.

O grão, o dicionário de dados, as decisões de modelagem e a explicação das tasks
foram preservados como documentação técnica, embora o artigo que os abrigaria
esteja fora do escopo. O enunciado diz que não é necessário desenvolver dashboard.

## O que faltava ou estava incorreto e como foi corrigido

### 1. Extração completa apagava a staging

Antes: cada extração executava TRUNCATE e recarregava toda a tabela; os DDLs da
staging executavam DROP TABLE CASCADE. Isso não cumpria o processamento
incremental e podia apagar dados úteis durante uma falha.

Correção: DDL idempotente com CREATE IF NOT EXISTS; remoção de DROP e TRUNCATE do
fluxo de produção; extração filtrada na origem e UPSERT com comparação de
conteúdo. Staging, watermark e log ficam na mesma transação. Os únicos TRUNCATE
dos testes são em bancos temporários criados pela suíte.

### 2. Filtro incremental usava a data de execução

Antes: ModifiedDate >= '{{ ds }}' no DW, sem checkpoint persistido, sem bootstrap
e sem limite superior. Rodar o DAG em 2026 podia ignorar os dados históricos;
uma execução após dias sem rodar podia perder modificações intermediárias.

Correção: etl.watermark armazena o limite da última extração bem-sucedida; o
bootstrap lê o histórico. Execuções posteriores usam o relógio SQL Server e a
marca anterior menos um dia. A marca só avança após commit. O DW usa diferença
de conteúdo, permitindo retomada mesmo após a staging já ter sido confirmada.

### 3. Pipeline dependia de quatro execuções manuais

Antes: criação da staging, extração, criação do DW e carga incremental eram
DAGs separados sem orquestração entre eles. A fato podia rodar sem dimensões,
sem dados atuais ou sem calendário para novas datas.

Correção: o DAG diário reúne todas essas etapas, validação e indicadores. O
calendário é ampliado após a extração, antes da carga da fato. Os DAGs manuais
foram mantidos como ferramentas de manutenção.

### 4. Dinheiro e descontos perdiam precisão

Antes: preços, custos, proporção de desconto e LineTotal eram limitados a duas
casas. Isso arredondava inclusive descontos fracionários e comprometia receita,
custo, lucro e consistência da fórmula de venda.

Correção: NUMERIC(19,4) para preços/custos/descontos e NUMERIC(38,6) para receita,
na staging e na fato. Foi aplicada migração aditiva e reextraído o histórico
para recuperar precisão já perdida. A receita final é exatamente igual à origem.

### 5. Clientes eram identificados por um número em vez do nome

Antes: nome_cliente recebia PersonID convertido em texto ou a palavra 'Loja'.

Correção: enriquecimento a partir de Person.Person e Sales.Store; tipo Loja
quando StoreID está preenchido. O maior timestamp dessas três entidades permite
capturar também alterações apenas no nome. Novos nomes foram carregados para
19.820 clientes.

### 6. Alterações indiretas podiam ficar desatualizadas

Antes: mudanças só no nome de categoria/subcategoria não alteravam ModifiedDate
do produto e podiam ficar invisíveis para a dimensão. Filtros por data e INNER
JOIN com território também podiam impedir atualização ou descartar vendas.

Correção: comparação das projeções completas das dimensões/fato antes do UPSERT;
mudanças do cabeçalho e classificações relacionadas são propagadas. Território
ausente usa membro explícito -1, sem perder o item.

### 7. Reexecução regravava dados inalterados

Antes: conflitos atualizavam registros e data_carga mesmo sem mudança real.

Correção: seleção prévia de novos/diferentes e proteção adicional por IS DISTINCT
FROM no conflito. Os testes verificam estabilidade da quantidade, da chave
substituta e da data_carga dos fatos inalterados.

### 8. Indicadores não tratavam denominador zero e custo era ambíguo

Antes: margem e ticket podiam falhar em cenários de soma zero; StandardCost atual
era apresentado sem ressalva como custo/lucro.

Correção: NULLIF nos denominadores, somas com COALESCE, ordenação determinística
e agrupamento de ranking por ID/nome. Custo, lucro e margem são explicitamente
estimados com StandardCost atual, sem alegar custo contábil histórico.

### 9. Faltavam validações e provas reproduzíveis

Antes: não havia suíte para bootstrap, reexecução, mudanças e falhas nem tarefa
de reconciliação da fato com staging.

Correção: scripts de integração reais e tarefa validate_dw que falha em caso de
staging vazia, contagens, receitas ou medidas divergentes. As FKs continuam
impedindo referências órfãs. Resultados/logs foram preservados em evidencias/.

### 10. Ambiente e documentação não eram suficientes para reproduzir a entrega

Antes: README explicava principalmente infraestrutura; as conexões dependiam
do cadastro local e o exemplo de dependências omitia o provedor MSSQL.

Correção: conexões declaradas no Compose, provedor MSSQL no exemplo, imports
compatíveis com Airflow 3, inicialização com interrupção se a migração falhar,
README refeito e documentação de OLTP, grão, dicionário, fluxo e dez KPIs.
Segredos e backup foram mantidos fora do Git. A afirmação de conforto em 8 GB
foi substituída por uma explicação do consumo combinado dos serviços.

## Testes executados e resultados

Base real, cinco verificações aprovadas:

1. Carga histórica completa: 121.317 itens.
2. Receita exata no SQL Server e no PostgreSQL: 109846381.399888.
3. Reexecução sem duplicação.
4. Reexecução sem modificar data_carga dos fatos inalterados.
5. Origem sem alterações: zero linhas históricas reextraídas na rodada seguinte.

Bases isoladas, dez verificações aprovadas:

1. Bootstrap a partir de um PostgreSQL vazio.
2. Inserção incremental: quantidade passa de 121.317 para 121.318.
3. Pedido com território nulo preserva o item e usa o membro desconhecido.
4. Atualização da quantidade preserva sk_venda e altera as medidas corretas.
5. Mudança apenas no cabeçalho expande calendário e atualiza a data da venda.
6. Mudança apenas na categoria é propagada para os produtos relacionados.
7. Falha proposital na staging reverte dados e não avança watermark.
8. Retomada após retirar a falha carrega a alteração pendente.
9. Repetição com sobreposição não duplica nem atualiza fatos iguais.
10. As dez consultas de KPI executam com fato vazia, sem divisão por zero.

Adicionalmente: docker compose config --quiet aprovado; nenhum erro de
importação dos DAGs; DAG completo executado com 13/13 tarefas em success.
As bases temporárias dos testes foram removidas. Não foram inseridos registros
de teste na base AdventureWorks original.

Evidências: validacao_base_real.json, validacao_isolada.json, teste_incremental.log,
dag_test.log e resultados.json, todos em docs/evidencias/. Os scripts para
repetição estão em scripts/ e os comandos no README.

## Resultados dos indicadores e padrões observados

Valores exatos na moeda da amostra; arredondamento apenas para apresentação.

- Faturamento líquido: **109.846.381,399888**.
- Custo estimado: **100.474.477,7735**.
- Lucro bruto estimado: **9.371.903,626388**.
- Margem bruta estimada: **8,53%**.
- Pedidos distintos: **31.465**.
- Ticket médio: **3.491,0656729664071190** por pedido.
- Unidades vendidas: **274.914**.
- Receita por categoria: Bikes **94.651.172,704731**; Components
  **11.802.593,286430**; Clothing **2.120.542,524801**; Accessories **1.272.072,883926**.
- Maior território em receita: **Southwest**, **24.184.609,600810**. Os dez
  territórios e respectivos valores estão em resultados.json (indicador 9).
- Produto de maior lucro estimado: ID **783**, **Mountain-200 Black, 42**,
  lucro **674.216,578641**. O ranking completo de dez produtos está no indicador 10.

Bikes representa aproximadamente **86,17%** da receita e Southwest **22,02%**.
Isso evidencia concentração comercial por categoria e território. Os seis
primeiros produtos no ranking de lucro pertencem à família Mountain-200.
Março/2014 é o mês de maior receita no conjunto, **7.217.531,091974**; isoladamente
esse pico não comprova sazonalidade. Essas conclusões são inferências descritivas
dos agregados executados, não previsões ou relações causais.

O DW final contém 504 produtos, 19.820 clientes, 11 membros de território
(10 reais e um desconhecido), 2.254 datas e 121.317 itens de venda.

## Limites operacionais que permanecem explícitos

- A origem deve atualizar ModifiedDate; alterações retrodatadas fora da janela
  exigem reextração da tabela. Exclusão física não é capturada porque não foi
  exigida no recorte de novos/modificados.
- A leitura das tabelas não constitui snapshot distribuído. Em origem com
  escritas concorrentes, uma falha de reconciliação exige nova execução.
- As dimensões são SCD1 e o custo é atual/estimado, decisões coerentes com os
  indicadores escolhidos e explicitadas para evitar interpretação histórica.
- O ambiente existente foi testado, inclusive banco analítico vazio. Não foi
  apagado o ambiente Docker original para simular um download novo de imagens.
- Artigo e prints estão excluídos da avaliação. Apresentação individual e nota
  dependem da avaliação do professor.

## Preservação e entrega

Foi salvo backup anterior das tabelas analíticas/staging em
data/backup/antes_auditoria.dump. A pasta e os segredos .env estão ignorados pelo
Git. O arquivo original data/Star Schema.png foi preservado.

Repositório confirmado: https://github.com/davs22/ApacheAirflow-e-ETL.
O relatório, a documentação, o diagrama, os SQLs e os scripts de teste compõem
um único conjunto de entrega; publicar apenas o README antigo não representa
esta versão corrigida.

Branch de entrega: `codex/conformidade-professor-2026-11-13`.

**Pendência de publicação:** a branch foi preparada localmente. O envio ao
repositório público foi bloqueado pela revisão automática de aprovação,
que exigiu autorização específica para publicar logs, resultados derivados do
banco e configurações. O envio não foi executado. É necessário aprovar essa
publicação para que o GitHub reflita todas as correções desta auditoria.
