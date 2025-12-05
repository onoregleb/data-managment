"""
Blockchain Wallet Service.

FastAPI сервис для сбора данных о кошельках Ethereum
и сохранения транзакций в MongoDB.

Основные функции:
- Добавление кошельков для мониторинга
- Загрузка транзакций через Etherscan API
- Получение статистики по кошелькам и транзакциям

Author: Gleb Onore
Version: 1.0.0
"""

import os
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient

# ==============================================================================
# Конфигурация приложения
# ==============================================================================

app = FastAPI(
    title="Blockchain Wallet Service",
    description="API для сбора и анализа данных Ethereum кошельков",
    version="1.0.0",
)

# MongoDB connection settings
# Переменные окружения позволяют настраивать подключение без изменения кода
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:mongo@localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "blockchain_raw")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Collections для хранения данных
wallets_collection = db["wallets"]
transactions_collection = db["transactions"]

# Etherscan API configuration
# Бесплатный ключ можно получить на https://etherscan.io/apis
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YourApiKeyToken")
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1  # Ethereum Mainnet


# ==============================================================================
# Pydantic Models для валидации данных
# ==============================================================================


class WalletRequest(BaseModel):
    """
    Модель запроса для добавления кошелька.

    Attributes:
        address: Ethereum адрес кошелька (формат 0x...)
    """

    address: str


class TransactionData(BaseModel):
    """
    Модель данных транзакции.

    Attributes:
        hash: Уникальный хеш транзакции
        from_address: Адрес отправителя
        to_address: Адрес получателя
        value: Сумма в ETH
        timestamp: Время транзакции
    """

    hash: str
    from_address: str
    to_address: str
    value: float
    timestamp: datetime


# ==============================================================================
# API Endpoints
# ==============================================================================


@app.get("/")
def root():
    """
    Главная страница API.

    Returns:
        dict: Информация о сервисе и доступных endpoints
    """
    return {
        "service": "Blockchain Wallet Service",
        "status": "running",
        "endpoints": ["/health", "/wallets", "/wallets/{address}", "/stats"],
    }


@app.get("/health")
def health():
    """
    Проверка здоровья сервиса.

    Проверяет подключение к MongoDB и возвращает статус.

    Returns:
        dict: Статус сервиса и подключения к БД
    """
    try:
        client.admin.command("ping")
        return {"status": "healthy", "mongodb": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/wallets")
def add_wallet(request: WalletRequest):
    """
    Добавить кошелек для отслеживания.

    Валидирует Ethereum адрес и сохраняет в MongoDB.
    Использует upsert для обновления существующих записей.

    Args:
        request: WalletRequest с адресом кошелька

    Returns:
        dict: Результат операции с информацией о кошельке

    Raises:
        HTTPException: 400 если адрес невалидный
    """
    address = request.address.lower()

    # Валидация формата Ethereum адреса
    # Адрес должен начинаться с 0x и иметь длину 42 символа
    if not address.startswith("0x") or len(address) != 42:
        raise HTTPException(status_code=400, detail="Invalid Ethereum address")

    wallet_data = {
        "address": address,
        "added_at": datetime.utcnow(),
        "last_updated": datetime.utcnow(),
        "transaction_count": 0,
        "balance_eth": 0.0,
    }

    # Upsert: создаём новую запись или обновляем существующую
    result = wallets_collection.update_one({"address": address}, {"$set": wallet_data}, upsert=True)

    return {
        "message": "Wallet added successfully",
        "address": address,
        "is_new": result.upserted_id is not None,
    }


@app.get("/wallets")
def get_wallets(limit: int = 100):
    """
    Получить список всех отслеживаемых кошельков.

    Args:
        limit: Максимальное количество записей (default: 100)

    Returns:
        dict: Количество и список кошельков
    """
    wallets = list(wallets_collection.find().limit(limit))
    for w in wallets:
        w["_id"] = str(w["_id"])

    return {"count": wallets_collection.count_documents({}), "wallets": wallets}


@app.get("/wallets/{address}")
def get_wallet(address: str):
    """
    Получить информацию о конкретном кошельке.

    Args:
        address: Ethereum адрес кошелька

    Returns:
        dict: Данные кошелька

    Raises:
        HTTPException: 404 если кошелек не найден
    """
    wallet = wallets_collection.find_one({"address": address.lower()})
    if not wallet:
        raise HTTPException(status_code=404, detail="Wallet not found")

    wallet["_id"] = str(wallet["_id"])
    return wallet


@app.post("/wallets/{address}/fetch")
async def fetch_wallet_data(address: str):
    """
    Загрузить транзакции кошелька с Etherscan API.

    Получает последние 100 транзакций и сохраняет в MongoDB.
    Автоматически добавляет кошелек если его нет в базе.

    Args:
        address: Ethereum адрес кошелька

    Returns:
        dict: Результат загрузки с количеством сохранённых транзакций
    """
    address = address.lower()

    # Автоматически добавляем кошелек если его нет
    wallet = wallets_collection.find_one({"address": address})
    if not wallet:
        wallets_collection.insert_one(
            {
                "address": address,
                "added_at": datetime.utcnow(),
                "last_updated": datetime.utcnow(),
                "transaction_count": 0,
            }
        )

    try:
        async with httpx.AsyncClient() as client_http:
            # Запрос к Etherscan API v2
            # Документация: https://docs.etherscan.io/
            response = await client_http.get(
                ETHERSCAN_BASE_URL,
                params={
                    "chainid": CHAIN_ID,
                    "module": "account",
                    "action": "txlist",
                    "address": address,
                    "startblock": 0,
                    "endblock": 99999999,
                    "page": 1,
                    "offset": 100,  # Последние 100 транзакций
                    "sort": "desc",
                    "apikey": ETHERSCAN_API_KEY,
                },
                timeout=30.0,
            )

            data = response.json()

            if data["status"] != "1":
                return {
                    "message": "No transactions found or API error",
                    "address": address,
                    "error": data.get("message", "Unknown error"),
                }

            transactions = data.get("result", [])
            saved_count = 0

            for tx in transactions:
                # Трансформация данных из Etherscan формата
                # value в wei конвертируем в ETH (1 ETH = 10^18 wei)
                tx_data = {
                    "hash": tx["hash"],
                    "wallet_address": address,
                    "from_address": tx["from"].lower(),
                    "to_address": tx["to"].lower() if tx["to"] else None,
                    "value_wei": int(tx["value"]),
                    "value_eth": int(tx["value"]) / 1e18,
                    "gas_used": int(tx["gasUsed"]),
                    "gas_price": int(tx["gasPrice"]),
                    "timestamp": datetime.fromtimestamp(int(tx["timeStamp"])),
                    "block_number": int(tx["blockNumber"]),
                    "is_error": tx["isError"] == "1",
                    "fetched_at": datetime.utcnow(),
                }

                # Upsert для идемпотентности - повторный запуск не создаст дубли
                transactions_collection.update_one(
                    {"hash": tx["hash"]}, {"$set": tx_data}, upsert=True
                )
                saved_count += 1

            # Обновляем метаданные кошелька
            wallets_collection.update_one(
                {"address": address},
                {"$set": {"last_updated": datetime.utcnow(), "transaction_count": saved_count}},
            )

            return {
                "message": "Data fetched successfully",
                "address": address,
                "transactions_saved": saved_count,
            }

    except Exception as e:
        return {"message": "Error fetching data", "address": address, "error": str(e)}


@app.get("/transactions")
def get_transactions(limit: int = 100, skip: int = 0):
    """
    Получить список транзакций с пагинацией.

    Args:
        limit: Количество записей на страницу (default: 100)
        skip: Количество записей для пропуска (default: 0)

    Returns:
        dict: Общее количество и список транзакций
    """
    txs = list(transactions_collection.find().skip(skip).limit(limit))
    for tx in txs:
        tx["_id"] = str(tx["_id"])
        if "timestamp" in tx:
            tx["timestamp"] = tx["timestamp"].isoformat()
        if "fetched_at" in tx:
            tx["fetched_at"] = tx["fetched_at"].isoformat()

    return {"count": transactions_collection.count_documents({}), "transactions": txs}


@app.get("/stats")
def get_stats():
    """
    Получить общую статистику по данным.

    Использует MongoDB aggregation pipeline для расчёта метрик.

    Returns:
        dict: Статистика по кошелькам и транзакциям
    """
    wallet_count = wallets_collection.count_documents({})
    tx_count = transactions_collection.count_documents({})

    # MongoDB aggregation для расчёта объёмных метрик
    # $group с _id: null агрегирует все документы
    pipeline = [
        {
            "$group": {
                "_id": None,
                "total_volume_eth": {"$sum": "$value_eth"},
                "avg_value_eth": {"$avg": "$value_eth"},
                "max_value_eth": {"$max": "$value_eth"},
            }
        }
    ]

    volume_stats = list(transactions_collection.aggregate(pipeline))

    return {
        "wallets_count": wallet_count,
        "transactions_count": tx_count,
        "volume_stats": volume_stats[0] if volume_stats else {},
    }


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
