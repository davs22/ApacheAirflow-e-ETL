
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

## Comandos uteis

```powershell
docker compose ps
docker compose logs -f airflow-scheduler
docker compose down
```

O ambiente usa `LocalExecutor` e nao inicia Redis ou workers separados. Os limites de memoria por servico mantem o consumo controlado; a carga real varia conforme os DAGs executados.
