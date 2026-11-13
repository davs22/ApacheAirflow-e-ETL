# Data Warehouse AdventureWorks com Apache Airflow e PostgreSQL

Pipeline de vendas: SQL Server AdventureWorks2019 → staging PostgreSQL → Star
Schema → validação → dez indicadores SQL. Airflow 3.3.1, PostgreSQL 16 e SQL
Server 2019 em Docker, com LocalExecutor e sem Redis.

## Inicialização

1. Copie `.env.example` para `.env` caso ainda não tenha o arquivo. Defina os
   segredos Fernet, API e JWT e as senhas. Não substitua um `.env` já configurado.
   A chave Fernet pode ser gerada com `cryptography.fernet.Fernet.generate_key()`
   em um Python com cryptography. API/JWT devem ser segredos aleatórios longos.
2. Execute `docker compose up -d`. O Compose migra o banco Airflow, cria o usuário,
   baixa o backup oficial AdventureWorks2019 e o restaura automaticamente.
3. Aguarde `docker compose ps --all`: `sqlserver-init` e `airflow-init` devem
   terminar com código 0; PostgreSQL, SQL Server e API devem estar saudáveis.
4. Abra http://localhost:8080 usando o usuário/senha de `.env`. Ative e dispare
   `etl_incremental_adventureworks`. Depois ele roda diariamente em UTC.

As conexões `postgres_default` e `mssql_adventureworks` vêm do Compose; não é
necessário cadastrá-las na interface. A senha SQL Server é `MSSQL_SA_PASSWORD`.
Os provedores necessários são instalados pela configuração de dependências;
`apache-airflow-providers-microsoft-mssql` é indispensável.
Se personalizar `_PIP_ADDITIONAL_REQUIREMENTS`, mantenha os provedores postgres,
microsoft-mssql e standard, além de sqlparse (já incluído no Airflow).
O primeiro início precisa de internet para imagens, dependências e backup.

## Executar e conferir

O DAG principal é autossuficiente: cria estruturas sem apagá-las, extrai mudanças,
carrega dimensões/calendário e fato, valida integridade e executa os dez KPIs.
Os três DAGs auxiliares são manuais e opcionais. Não os rode em paralelo com o
principal durante manutenção.

```powershell
docker compose ps --all
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose exec -T airflow-scheduler airflow dags test etl_incremental_adventureworks
docker compose logs -f airflow-scheduler
```

Para repetir a auditoria real (os scripts são copiados para `/tmp` do container):

```powershell
docker compose cp scripts/run_validation.py airflow-scheduler:/tmp/run_validation.py
docker compose exec -T airflow-scheduler python /tmp/run_validation.py
docker compose cp scripts/test_incremental_isolated.py airflow-scheduler:/tmp/test_incremental_isolated.py
docker compose exec -T airflow-scheduler python /tmp/test_incremental_isolated.py
docker compose cp scripts/export_evidence.py airflow-scheduler:/tmp/export_evidence.py
docker compose exec -T airflow-scheduler python /tmp/export_evidence.py
```

A suíte isolada cria e remove bancos temporários próprios; precisa das permissões
administrativas já existentes no ambiente local. A base original não recebe
inserções ou alterações de teste. A validação real pressupõe a amostra oficial
sem alterações (121.317 itens).

## Arquivos e documentação

- `dags/etl_incremental_vendas.py`: fluxo completo com 13 tarefas.
- `dags/aw_pipeline.py`: extração incremental transacional e execução SQL.
- `dags/sql/`: DDL, migração, cargas, qualidade e dez indicadores.
- [Documentação técnica](docs/documentacao_tecnica.md): análise OLTP, grão,
  dicionário, decisões, regras de indicadores e limites da estratégia incremental.
- [Diagrama estrela](docs/modelo_estrela.svg) e [fonte Mermaid](docs/modelo_estrela.mmd).
- [Relatório de conformidade](docs/relatorio_conformidade.md): lacunas iniciais,
  correções, requisitos e resultados dos testes.
- `docs/evidencias/`: logs e resultados JSON da auditoria.

O artigo e os prints de execução foram excluídos do escopo por solicitação do
usuário. Nenhum dashboard é necessário para a atividade.

## Conectar pelo DBeaver

PostgreSQL: host `localhost`, porta `5433` (ou POSTGRES_HOST_PORT), banco,
usuário e senha de `.env`. Entre containers, use `postgres:5432`.
SQL Server: `localhost:1433`, banco AdventureWorks2019, usuário `sa` e senha
MSSQL_SA_PASSWORD. Não publique `.env`, backups ou senhas.

## Operação

As pastas dags, logs, plugins, config e data são montadas em `/opt/airflow`.
Os bancos persistem em volumes Docker. `docker compose down` para os serviços
preserva os volumes; não use `down -v` se deseja manter os dados.
O conjunto de limites de memória aproxima-se de 7 GB: além disso há o sistema
operacional, Docker e dependências, então 8 GB totais não garantem operação
confortável. Reduza paralelismo ou aumente memória disponível em caso de pressão.

Repositório: https://github.com/davs22/ApacheAirflow-e-ETL
