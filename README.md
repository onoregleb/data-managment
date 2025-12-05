# Blockchain Data Pipeline

ETL-пайплайн для сбора и анализа данных Ethereum блокчейна.

**Архитектура:** FastAPI → MongoDB → Airflow → PostgreSQL → DBT → DataMart

На основе [blockchain_app](https://github.com/onoregleb/blockchain_app)

---

## 🌐 Production (Cloud.ru VM)

| Сервис | URL | Credentials |
|--------|-----|-------------|
| **Airflow** | http://213.171.27.223:8080 | admin / admin |
| **FastAPI** | http://213.171.27.223:8000/docs | - |
| **PostgreSQL DWH** | 213.171.27.223:5433 | postgres / postgres |
| **MongoDB** | 213.171.27.223:27017 | mongo / mongo |

---

## 📁 Структура проекта

```
blockchain-pipeline/
├── airflow/
│   └── dags/
│       └── blockchain_etl_pipeline.py  # Основной DAG
├── app/
│   ├── main.py              # FastAPI приложение
│   ├── Dockerfile
│   └── requirements.txt
├── dbt/
│   ├── dbt_project.yml      # Конфигурация DBT
│   ├── profiles.yml         # Подключение к PostgreSQL
│   └── models/
│       ├── staging/         # STG слой (views)
│       ├── ods/             # ODS слой (incremental)
│       ├── dwh/             # DWH слой (tables)
│       └── marts/           # Витрины (tables)
├── dwh/
│   └── 01_init.sql          # Инициализация PostgreSQL
├── docker-compose.yml       # Оркестрация контейнеров
├── .pre-commit-config.yaml  # Pre-commit хуки
├── pyproject.toml           # Конфигурация Python tools
└── .sqlfluff                # Конфигурация SQL linter
```

---

## 🚀 Запуск

### Локально

```bash
cd blockchain-pipeline
docker compose up -d
```

### На сервере

```bash
ssh user1@213.171.27.223
cd ~/data-managment
git pull
docker compose up -d
```

---

## 🔄 Data Pipeline

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Etherscan  │────▶│   MongoDB   │────▶│  PostgreSQL │────▶│     DBT     │
│    API      │     │  (raw data) │     │    (STG)    │     │  (ODS/DWH)  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                    │                    │
                          ▼                    ▼                    ▼
                    wallets,            wallets,             dim_wallets,
                    transactions        transactions         fact_transactions,
                                                             marts/*
```

**Расписание:** каждые 5 минут (Airflow DAG)

---

## 📊 DBT Модели

### Слои данных

| Слой | Materialization | Описание |
|------|-----------------|----------|
| **staging** | view | Сырые данные, минимальные трансформации |
| **ods** | incremental | Очищенные данные, дедупликация |
| **dwh** | table | Dimensional model (dim/fact) |
| **marts** | table | Аналитические витрины |

### Модели

- `stg_wallets`, `stg_transactions` — staging
- `ods_wallets`, `ods_transactions` — ODS с оконными функциями
- `dim_wallets`, `fact_transactions` — DWH
- `mart_wallet_analytics`, `mart_daily_metrics`, `mart_summary` — витрины

---

## 🛠 Development

### Pre-commit hooks

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

### GitHub Actions CI/CD

При push в `main`/`master` автоматически:
1. Проверяется код (black, isort, flake8, sqlfluff)
2. Тестируются DBT модели
3. Деплоится на сервер через SSH

### Запуск DBT

```bash
docker exec -it airflow-scheduler bash
cd /opt/airflow/dbt
dbt run
dbt test
```

---

## 📝 API Endpoints

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

## 🗄 SQL Примеры

### Оконные функции в ODS

```sql
-- Кумулятивная сумма и скользящее среднее
SELECT
    wallet_address,
    value_eth,
    SUM(value_eth) OVER (PARTITION BY wallet_address ORDER BY tx_timestamp) AS cumulative_eth,
    AVG(value_eth) OVER (PARTITION BY wallet_address ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma_10
FROM ods_transactions;
```

### Аналитика из витрин

```sql
-- Топ кошельков по объёму
SELECT wallet_address, total_volume_eth, volume_rank
FROM mart_wallet_analytics
ORDER BY volume_rank
LIMIT 10;
```

---

## 🔧 Ресурсы

| Сервис | RAM | vCPU |
|--------|-----|------|
| MongoDB | 768 MB | 0.35 |
| PostgreSQL DW | 384 MB | 0.20 |
| PostgreSQL Airflow | 256 MB | 0.15 |
| App (FastAPI) | 256 MB | 0.20 |
| Airflow Webserver | 768 MB | 0.40 |
| Airflow Scheduler | 896 MB | 0.50 |
| **Итого** | **~3.3 GB** | **1.8** |

---

## 📄 License

MIT
