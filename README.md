# Blockchain Data Pipeline

ETL-пайплайн для сбора и анализа данных Ethereum блокчейна.

**Архитектура:** FastAPI → MongoDB → Airflow → PostgreSQL

---

## Production (Cloud.ru VM)

| Сервис | URL | Credentials |
|--------|-----|-------------|
| **Airflow** | http://213.171.27.223:8080 | admin / admin |
| **FastAPI** | http://213.171.27.223:8000/docs | - |
| **PostgreSQL DWH** | 213.171.27.223:5433 | postgres / postgres |
| **MongoDB** | 213.171.27.223:27017 | mongo / mongo |

---

## Структура проекта

```
blockchain-pipeline/
├── airflow/
│   └── dags/
│       └── blockchain_etl_pipeline.py  # Основной DAG
├── app/
│   ├── main.py              # FastAPI приложение
│   ├── Dockerfile
│   └── requirements.txt
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

## Data Pipeline

```
Etherscan API -> MongoDB (raw data) -> PostgresSQL
```

**Расписание:** каждые 5 минут (Airflow DAG)

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

## SQL Примеры

### Оконные функции

```sql
-- Кумулятивная сумма и скользящее среднее
SELECT
    wallet_address,
    value_eth,
    SUM(value_eth) OVER (PARTITION BY wallet_address ORDER BY timestamp) AS cumulative_eth,
    AVG(value_eth) OVER (PARTITION BY wallet_address ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma_10
FROM transactions;
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

| Сервис | RAM | vCPU |
|--------|-----|------|
| MongoDB | 768 MB | 0.35 |
| PostgreSQL DW | 384 MB | 0.20 |
| PostgreSQL Airflow | 256 MB | 0.15 |
| App (FastAPI) | 256 MB | 0.20 |
| Airflow Webserver | 768 MB | 0.40 |
| Airflow Scheduler | 896 MB | 0.50 |
| Airflow Init | 512 MB | 0.20 |
| **Итого** | **~3.8 GB** | **2.0** |

---
