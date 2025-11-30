# Blockchain Data Pipeline

EL-пайплайн: FastAPI → MongoDB → Airflow → PostgreSQL

На основе [blockchain_app](https://github.com/onoregleb/blockchain_app)


## Запуск

```powershell
cd C:\Users\Gleb Onore\Desktop\db-course\marketplace-pipeline
docker compose up -d
```

## Сервисы

| Сервис | URL |
|--------|-----|
| **FastAPI** | http://localhost:8000/docs |
| **Airflow** | http://localhost:8080 (admin/admin) |

## Использование

### 1. Добавить кошелек (FastAPI → MongoDB)

```bash
curl -X POST "http://localhost:8000/wallets" \
  -H "Content-Type: application/json" \
  -d '{"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"}'
```

### 2. Загрузить транзакции

```bash
curl -X POST "http://localhost:8000/wallets/0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045/fetch"
```

### 3. Посмотреть данные

```bash
curl http://localhost:8000/wallets
curl http://localhost:8000/transactions
curl http://localhost:8000/stats
```

### 4. Проверить PostgreSQL

```powershell
docker exec -it postgres-dw psql -U postgres -d blockchain -c "SELECT * FROM transactions;"
```

## Остановка

```powershell
docker compose down
```
