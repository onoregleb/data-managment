# Blockchain Data Pipeline

ETL-пайплайн для сбора и анализа данных Ethereum блокчейна.

**Архитектура:** FastAPI → MongoDB → Airflow → PostgreSQL → DBT

---

## Production (Cloud.ru VM)

| Сервис | URL | Credentials |
|--------|-----|-------------|
| **Airflow** | http://213.171.31.111:8080 | admin / admin |
| **FastAPI** | http://213.171.31.111:8000/docs | - |
| **PostgreSQL DWH** | 213.171.31.111:5433 | postgres / postgres |
| **MongoDB** | 213.171.31.111:27017 | mongo / mongo |
| **EDR Report (Elementary)** | http://213.171.31.111:8090/ | - |

---

## Подключение к базам данных

### MongoDB

**Формат строки подключения:**
```
mongodb://[username:password@]host[:port][/database][?options]
```

**Переменные окружения (пример):**
```bash
MONGO_INITDB_ROOT_USERNAME=mongo
MONGO_INITDB_ROOT_PASSWORD=mongo
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_INITDB_DATABASE=blockchain_raw
MONGO_URL=mongodb://mongo:mongo@localhost:27017/blockchain_raw
```

**Локально (Docker Compose):**
```bash
MONGO_URI=mongodb://mongo:mongo@mongodb:27017/
MONGO_DB=blockchain_raw
```

**Production:**
```bash
MONGO_URI=mongodb://mongo:mongo@213.171.31.111:27017/
MONGO_HOST=213.171.31.111
MONGO_PORT=27017
MONGO_INITDB_ROOT_USERNAME=mongo
MONGO_INITDB_ROOT_PASSWORD=mongo
MONGO_INITDB_DATABASE=blockchain_raw
MONGO_URL=mongodb://mongo:mongo@213.171.31.111:27017/blockchain_raw
```

### PostgreSQL

**Формат строки подключения:**
```
postgresql://[username:password@]host[:port][/database][?options]
```

**Переменные окружения (пример):**
```bash
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=blockchain
POSTGRES_URI=postgresql://postgres:postgres@localhost:5432/blockchain
```

**Примечание:** Для локального Docker Compose используется внутренний порт 5432, для внешнего доступа - 5433.

**Локально (Docker Compose):**
```bash
POSTGRES_URI=postgresql://postgres:postgres@postgres-dw:5432/blockchain
POSTGRES_HOST=postgres-dw
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=blockchain
```

**Production:**
```bash
POSTGRES_URI=postgresql://postgres:postgres@213.171.31.111:5433/blockchain
POSTGRES_HOST=213.171.31.111
POSTGRES_PORT=5433
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=blockchain
```

**Подключение через DBeaver / pgAdmin:**
- Host: `213.171.31.111`
- Port: `5433`
- Database: `blockchain`
- Username: `postgres`
- Password: `postgres`

---

## Структура проекта

```
blockchain-pipeline/
├── airflow/
│   └── dags/
│       ├── blockchain_etl_pipeline.py  # Основной DAG
│       ├── el_mongo_to_postgres.py    # EL Pipeline + DBT
│       └── dbt_pipeline.py            # DBT Pipeline
├── app/
│   ├── main.py              # FastAPI приложение
│   ├── Dockerfile
│   └── requirements.txt
├── dbt/                     # DBT проект
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/              # Пользовательские макросы
│   └── models/              # DBT модели (staging, ods, intermediate, marts)
├── dwh/
│   └── 01_init.sql          # Инициализация PostgreSQL
├── docker-compose.yml       # Оркестрация контейнеров
├── .pre-commit-config.yaml  # Pre-commit хуки
└── pyproject.toml           # Конфигурация Python tools
```

---

## Запуск

### Локально

```bash
cd blockchain-pipeline
docker compose up -d
```

---

## Elementary EDR report (как сделать доступным по ссылке и автообновлять)

### Как это работает

- **Генерация**: DAG `dbt_pipeline` выполняет таск `edr_report`, который запускает:
  - `edr report ... --target-path edr_reports`
  - и кладёт HTML + ассеты в `./dbt/edr_reports/` (внутри контейнера это `/opt/airflow/dbt/edr_reports`)
- **Публикация**: контейнер `edr-report` (Nginx) раздаёт `./dbt/edr_reports/` по HTTP
- **Автообновление**: каждый раз, когда в Airflow успешно проходит `edr_report`, Nginx сразу начинает
  отдавать обновлённую версию (кэш отключён заголовками в `edr_report_nginx.conf`)


### Ссылка для сдачи

- **Elementary edr report URL**: `http://213.171.31.111:8090/`


## Data Pipeline

```
Etherscan API -> MongoDB (raw data) -> PostgreSQL -> DBT (transformations)
```

**Расписание:**
- ETL Pipeline: каждые 30 минут (Airflow DAG `blockchain_etl_pipeline`)
- EL Pipeline: каждый час (Airflow DAG `el_mongo_to_postgres`)
- DBT Transformations: после каждой загрузки данных (dbt run + dbt test)
- DBT Pipeline: каждый час (Airflow DAG `dbt_pipeline`)

---

## Development

### GitHub Actions CI/CD

При push в `main`/`master` автоматически:
1. **lint** — Проверяется код (black, isort, flake8, sqlfluff)
2. **deploy** — Деплоится на сервер через SSH

---

## API Endpoints

### Добавить кошелек

```bash
curl -X POST "http://localhost:8000/wallets" \
  -H "Content-Type: application/json" \
  -d '{"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"}'
```

### Загрузить транзакции

```bash
curl -X POST "http://localhost:8000/wallets/0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045/fetch"
```

### Статистика

```bash
curl http://localhost:8000/stats
```

---

## DBT

Проект включает dbt для трансформации данных:
- **Staging модели**: Очистка и стандартизация сырых данных
- **ODS слой**: Валидированные таблицы с оконными метриками (`ods_wallets`, `ods_transactions`)
- **Intermediate модели**: Промежуточные агрегации
- **Marts модели**: Финальные таблицы для аналитики

Подробнее см. [dbt/README.md](dbt/README.md)

---

## SQL Примеры

### Оконные функции

```sql
-- ODS: накопительный объём и предыдущий timestamp по каждому кошельку
SELECT
    wallet_address,
    transaction_timestamp,
    SUM(value_eth) OVER (
        PARTITION BY wallet_address
        ORDER BY transaction_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_volume_eth,
    LAG(transaction_timestamp) OVER (
        PARTITION BY wallet_address
        ORDER BY transaction_timestamp
    ) AS prev_tx_timestamp
FROM ods_transactions;
```

### Аналитика

```sql
-- Топ кошельков по объёму
SELECT
    wallet_address,
    COUNT(*) as tx_count,
    SUM(value_eth) as total_volume
FROM transactions
GROUP BY wallet_address
ORDER BY total_volume DESC
LIMIT 10;
```

---

## Ресурсы

Таблица ниже соответствует лимитам контейнеров из `docker-compose.yml` (параметры `mem_limit` и `cpus`).
У тебя **8GB RAM**, поэтому по памяти есть запас (steady-state лимиты ~6.6GB + место под ОС/кэш).

| Сервис | RAM limit | vCPU limit |
|--------|-----|------|
| MongoDB | 1024 MB | 0.5 |
| PostgreSQL DW | 2048 MB | 1.0 |
| PostgreSQL Airflow | 512 MB | 0.5 |
| App (FastAPI) | 512 MB | 0.5 |
| Airflow Webserver | 1024 MB | 0.8 |
| Airflow Scheduler | 1536 MB | 1.0 |
| EDR Report (Nginx) | 128 MB | 0.2 |
| **Итого (steady-state)** | **~6.6 GB** | **4.5** |

---
