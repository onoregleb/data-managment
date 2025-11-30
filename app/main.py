"""
Blockchain Wallet Service
Сервис для сбора данных о кошельках Ethereum и сохранения в MongoDB
"""

import os
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import httpx

app = FastAPI(title="Blockchain Wallet Service", version="1.0.0")

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:mongo@localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "blockchain_raw")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Collections
wallets_collection = db["wallets"]
transactions_collection = db["transactions"]

# Etherscan API (бесплатный ключ можно получить на etherscan.io)
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YourApiKeyToken")
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"


# ==================== Models ====================

class WalletRequest(BaseModel):
    address: str


class TransactionData(BaseModel):
    hash: str
    from_address: str
    to_address: str
    value: float
    timestamp: datetime


# ==================== Endpoints ====================

@app.get("/")
def root():
    """Главная страница"""
    return {
        "service": "Blockchain Wallet Service",
        "status": "running",
        "endpoints": ["/health", "/wallets", "/wallets/{address}", "/stats"]
    }


@app.get("/health")
def health():
    """Проверка здоровья сервиса"""
    try:
        client.admin.command('ping')
        return {"status": "healthy", "mongodb": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/wallets")
def add_wallet(request: WalletRequest):
    """
    Добавить кошелек для отслеживания.
    Сохраняет информацию о кошельке в MongoDB.
    """
    address = request.address.lower()
    
    # Проверяем формат адреса
    if not address.startswith("0x") or len(address) != 42:
        raise HTTPException(status_code=400, detail="Invalid Ethereum address")
    
    # Создаем запись о кошельке
    wallet_data = {
        "address": address,
        "added_at": datetime.utcnow(),
        "last_updated": datetime.utcnow(),
        "transaction_count": 0,
        "balance_eth": 0.0
    }
    
    # Upsert - обновляем если существует
    result = wallets_collection.update_one(
        {"address": address},
        {"$set": wallet_data},
        upsert=True
    )
    
    return {
        "message": "Wallet added successfully",
        "address": address,
        "is_new": result.upserted_id is not None
    }


@app.get("/wallets")
def get_wallets(limit: int = 100):
    """Получить список всех кошельков"""
    wallets = list(wallets_collection.find().limit(limit))
    for w in wallets:
        w["_id"] = str(w["_id"])
    
    return {
        "count": wallets_collection.count_documents({}),
        "wallets": wallets
    }


@app.get("/wallets/{address}")
def get_wallet(address: str):
    """Получить информацию о конкретном кошельке"""
    wallet = wallets_collection.find_one({"address": address.lower()})
    if not wallet:
        raise HTTPException(status_code=404, detail="Wallet not found")
    
    wallet["_id"] = str(wallet["_id"])
    return wallet


@app.post("/wallets/{address}/fetch")
async def fetch_wallet_data(address: str):
    """
    Получить данные о транзакциях кошелька с Etherscan.
    Сохраняет транзакции в MongoDB.
    """
    address = address.lower()
    
    # Проверяем что кошелек добавлен
    wallet = wallets_collection.find_one({"address": address})
    if not wallet:
        # Добавляем автоматически
        wallets_collection.insert_one({
            "address": address,
            "added_at": datetime.utcnow(),
            "last_updated": datetime.utcnow(),
            "transaction_count": 0
        })
    
    try:
        async with httpx.AsyncClient() as client_http:
            # Получаем транзакции с Etherscan
            response = await client_http.get(
                ETHERSCAN_BASE_URL,
                params={
                    "module": "account",
                    "action": "txlist",
                    "address": address,
                    "startblock": 0,
                    "endblock": 99999999,
                    "page": 1,
                    "offset": 100,  # Последние 100 транзакций
                    "sort": "desc",
                    "apikey": ETHERSCAN_API_KEY
                },
                timeout=30.0
            )
            
            data = response.json()
            
            if data["status"] != "1":
                return {
                    "message": "No transactions found or API error",
                    "address": address,
                    "error": data.get("message", "Unknown error")
                }
            
            transactions = data.get("result", [])
            saved_count = 0
            
            for tx in transactions:
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
                    "fetched_at": datetime.utcnow()
                }
                
                # Upsert транзакции
                transactions_collection.update_one(
                    {"hash": tx["hash"]},
                    {"$set": tx_data},
                    upsert=True
                )
                saved_count += 1
            
            # Обновляем статистику кошелька
            wallets_collection.update_one(
                {"address": address},
                {"$set": {
                    "last_updated": datetime.utcnow(),
                    "transaction_count": saved_count
                }}
            )
            
            return {
                "message": "Data fetched successfully",
                "address": address,
                "transactions_saved": saved_count
            }
            
    except Exception as e:
        return {
            "message": "Error fetching data",
            "address": address,
            "error": str(e)
        }


@app.get("/transactions")
def get_transactions(limit: int = 100, skip: int = 0):
    """Получить все транзакции"""
    txs = list(transactions_collection.find().skip(skip).limit(limit))
    for tx in txs:
        tx["_id"] = str(tx["_id"])
        if "timestamp" in tx:
            tx["timestamp"] = tx["timestamp"].isoformat()
        if "fetched_at" in tx:
            tx["fetched_at"] = tx["fetched_at"].isoformat()
    
    return {
        "count": transactions_collection.count_documents({}),
        "transactions": txs
    }


@app.get("/stats")
def get_stats():
    """Общая статистика по данным"""
    wallet_count = wallets_collection.count_documents({})
    tx_count = transactions_collection.count_documents({})
    
    # Агрегация по объему транзакций
    pipeline = [
        {"$group": {
            "_id": None,
            "total_volume_eth": {"$sum": "$value_eth"},
            "avg_value_eth": {"$avg": "$value_eth"},
            "max_value_eth": {"$max": "$value_eth"}
        }}
    ]
    
    volume_stats = list(transactions_collection.aggregate(pipeline))
    
    return {
        "wallets_count": wallet_count,
        "transactions_count": tx_count,
        "volume_stats": volume_stats[0] if volume_stats else {}
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

