
# Ambiente ETL com Airflow e PostgreSQL

Ambiente local baseado no Apache Airflow 3.3.1 e PostgreSQL 16, otimizado para computadores com ate 8 GB de RAM.

## Inicializacao

1. Gere uma chave Fernet e um segredo da API e substitua os valores correspondentes em `.env`.
2. Inicialize o banco e os componentes do Airflow:

	```powershell
	docker compose up airflow-init
	docker compose up -d
	```

3. Abra `http://localhost:8080` e entre com as credenciais definidas em `.env`.

## Estrutura de pastas

Crie as pastas abaixo na raiz do projeto antes de iniciar os containers:

```text
.
|-- dags/
|-- logs/
|-- plugins/
|-- config/
|-- data/
|-- docker-compose.yml
|-- .env
`-- .env.example
```

- `dags/`: arquivos Python com os DAGs do Airflow.
- `logs/`: logs das execucoes das tarefas, montados no container em `/opt/airflow/logs`.
- `plugins/`: plugins personalizados do Airflow.
- `config/`: arquivos adicionais de configuracao do Airflow.
- `data/`: arquivos gerados ou consumidos pelos DAGs, montados no container em `/opt/airflow/data`.

O banco PostgreSQL usa o volume Docker `postgres-db-volume`; essa pasta nao precisa ser criada manualmente no projeto.

## Conectar ao PostgreSQL pelo DBeaver

O PostgreSQL escuta na porta `5432` dentro do Docker e e publicado no Windows pela porta `5433` (configuravel por `POSTGRES_HOST_PORT` no `.env`). No DBeaver, crie uma conexao PostgreSQL com estes dados:

| Campo | Valor |
|---|---|
| Host | `localhost` |
| Porta | `5433` |
| Banco de dados | valor de `POSTGRES_DB` no `.env` (`airflow` por padrao) |
| Usuario | valor de `POSTGRES_USER` no `.env` (`airflow` por padrao) |
| Senha | valor de `POSTGRES_PASSWORD` no `.env` |

Se alterar `POSTGRES_HOST_PORT`, use a nova porta no DBeaver. O nome de host `postgres` e a porta `5432` sao para conexoes entre containers; nao os use no DBeaver instalado no Windows. Certifique-se de que o servico esteja iniciado com `docker compose up -d`.

## Comandos uteis

```powershell
docker compose ps
docker compose logs -f airflow-scheduler
docker compose down
```

O ambiente usa `LocalExecutor` e nao inicia Redis ou workers separados. Os limites de memoria por servico mantem o consumo controlado; a carga real varia conforme os DAGs executados.
