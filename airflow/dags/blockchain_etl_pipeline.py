"""
Blockchain ETL Pipeline
Сбор данных ERC-20 токенов через Etherscan API → MongoDB → PostgreSQL
Запускается каждые 5 минут
"""

import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# USDT Token Contract на Ethereum
USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"

# Кошельки для мониторинга
MONITORED_WALLETS = [
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",  # Активный кошелек
    "0x28c6c06298d514db089934071355e5743bf21d60",  # Binance Hot Wallet
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",  # Binance
]

# Etherscan API V2 настройки
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YourApiKeyToken")
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1  # Ethereum Mainnet


def fetch_token_transfers(**context):
    """
    Сбор транзакций ERC-20 токена через Etherscan API.
    """
    import time

    import httpx
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")
    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    wallets_collection = db["wallets"]
    transactions_collection = db["transactions"]

    print(f"Fetching blockchain data for {len(MONITORED_WALLETS)} wallets")
    print(f"Using Etherscan API Key: {ETHERSCAN_API_KEY[:10]}...")

    total_transactions = 0
    wallets_found = set()

    try:
        with httpx.Client(timeout=60.0) as http_client:
            for wallet_address in MONITORED_WALLETS:
                print(f"\nProcessing wallet: {wallet_address}")

                # 1. Получаем обычные ETH транзакции (API V2)
                response = http_client.get(
                    ETHERSCAN_BASE_URL,
                    params={
                        "chainid": CHAIN_ID,
                        "module": "account",
                        "action": "txlist",
                        "address": wallet_address,
                        "page": 1,
                        "offset": 100,
                        "startblock": 0,
                        "endblock": 99999999,
                        "sort": "desc",
                        "apikey": ETHERSCAN_API_KEY,
                    },
                )

                data = response.json()

                if data.get("status") != "1":
                    error_msg = data.get("message", "Unknown error")
                    result_msg = data.get("result", "")
                    print(f"API response: {error_msg} - {result_msg}")

                    if "rate limit" in str(result_msg).lower():
                        print("Rate limited, waiting 5 seconds...")
                        time.sleep(5)
                    continue

                transactions = data.get("result", [])
                print(f"✅ Received {len(transactions)} ETH transactions")

                for tx in transactions:
                    # Определяем адреса
                    from_addr = tx.get("from", "").lower()
                    to_addr = tx.get("to", "").lower()

                    if from_addr:
                        wallets_found.add(from_addr)
                    if to_addr:
                        wallets_found.add(to_addr)

                    # Парсим значение (ETH = 18 decimals)
                    value_raw = int(tx.get("value", 0))
                    value_eth = value_raw / (10**18)

                    # Формируем документ транзакции
                    tx_data = {
                        "hash": tx.get("hash"),
                        "wallet_address": wallet_address,
                        "from_address": from_addr,
                        "to_address": to_addr,
                        "value_raw": str(value_raw),
                        "value_eth": value_eth,
                        "gas_used": int(tx.get("gasUsed", 0)),
                        "gas_price": int(tx.get("gasPrice", 0)),
                        "timestamp": datetime.fromtimestamp(int(tx.get("timeStamp", 0))),
                        "block_number": int(tx.get("blockNumber", 0)),
                        "is_error": tx.get("isError") == "1",
                        "token_name": "ETH",
                        "token_symbol": "ETH",
                        "fetched_at": datetime.utcnow(),
                    }

                    # Upsert транзакции
                    result = transactions_collection.update_one(
                        {"hash": tx["hash"]}, {"$set": tx_data}, upsert=True
                    )

                    if result.upserted_id or result.modified_count:
                        total_transactions += 1

                # 2. Получаем токен-транзакции
                time.sleep(0.25)  # Небольшая пауза между запросами

                response = http_client.get(
                    ETHERSCAN_BASE_URL,
                    params={
                        "chainid": CHAIN_ID,
                        "module": "account",
                        "action": "tokentx",
                        "address": wallet_address,
                        "page": 1,
                        "offset": 100,
                        "startblock": 0,
                        "endblock": 99999999,
                        "sort": "desc",
                        "apikey": ETHERSCAN_API_KEY,
                    },
                )

                token_data = response.json()

                if token_data.get("status") == "1" and token_data.get("result"):
                    token_txs = token_data["result"]
                    print(f"✅ Received {len(token_txs)} token transactions")

                    for tx in token_txs:
                        from_addr = tx.get("from", "").lower()
                        to_addr = tx.get("to", "").lower()

                        if from_addr:
                            wallets_found.add(from_addr)
                        if to_addr:
                            wallets_found.add(to_addr)

                        decimals = int(tx.get("tokenDecimal", 18))
                        value_raw = int(tx.get("value", 0))
                        value_normalized = value_raw / (10**decimals)

                        tx_data = {
                            "hash": tx.get("hash"),
                            "wallet_address": wallet_address,
                            "from_address": from_addr,
                            "to_address": to_addr,
                            "value_raw": str(value_raw),
                            "value_eth": value_normalized,
                            "gas_used": int(tx.get("gasUsed", 0)),
                            "gas_price": int(tx.get("gasPrice", 0)),
                            "timestamp": datetime.fromtimestamp(int(tx.get("timeStamp", 0))),
                            "block_number": int(tx.get("blockNumber", 0)),
                            "is_error": False,
                            "token_name": tx.get("tokenName", ""),
                            "token_symbol": tx.get("tokenSymbol", ""),
                            "contract_address": tx.get("contractAddress", ""),
                            "fetched_at": datetime.utcnow(),
                        }

                        result = transactions_collection.update_one(
                            {"hash": tx["hash"]}, {"$set": tx_data}, upsert=True
                        )

                        if result.upserted_id or result.modified_count:
                            total_transactions += 1

                # Пауза между кошельками для соблюдения rate limits
                time.sleep(0.25)

            # Сохраняем найденные кошельки
            for wallet_addr in wallets_found:
                if wallet_addr and len(wallet_addr) == 42:
                    tx_count = transactions_collection.count_documents(
                        {"$or": [{"from_address": wallet_addr}, {"to_address": wallet_addr}]}
                    )

                    wallets_collection.update_one(
                        {"address": wallet_addr},
                        {
                            "$set": {
                                "address": wallet_addr,
                                "transaction_count": tx_count,
                                "last_updated": datetime.utcnow(),
                            },
                            "$setOnInsert": {"added_at": datetime.utcnow()},
                        },
                        upsert=True,
                    )

    except httpx.TimeoutException:
        print("Request timeout - Etherscan API is slow")
    except Exception as e:
        print(f"Error fetching data: {str(e)}")

    client.close()

    print("\n" + "=" * 50)
    print("📥 FETCH COMPLETE")
    print("=" * 50)
    print(f"Transactions saved: {total_transactions}")
    print(f"Unique wallets found: {len(wallets_found)}")
    print("=" * 50 + "\n")

    return {"transactions": total_transactions, "wallets": len(wallets_found)}


def extract_from_mongodb(**context):
    """
    Инкрементальное извлечение данных из MongoDB.
    Извлекает только новые записи с момента последней синхронизации.
    """
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")
    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    # Коллекция для хранения состояния синхронизации
    sync_state = db["sync_state"]

    # Получаем время последней синхронизации
    last_sync = sync_state.find_one({"_id": "last_sync"})
    last_sync_time = last_sync.get("timestamp") if last_sync else None

    if last_sync_time:
        print(f"📅 Last sync: {last_sync_time}")
        wallet_filter = {"last_updated": {"$gt": last_sync_time}}
        tx_filter = {"fetched_at": {"$gt": last_sync_time}}
    else:
        print("📅 First sync - loading all data")
        wallet_filter = {}
        tx_filter = {}

    # Извлекаем только новые/обновленные кошельки
    wallets = list(db.wallets.find(wallet_filter))
    for w in wallets:
        w["_id"] = str(w["_id"])
        if "added_at" in w and hasattr(w["added_at"], "isoformat"):
            w["added_at"] = w["added_at"].isoformat()
        if "last_updated" in w and hasattr(w["last_updated"], "isoformat"):
            w["last_updated"] = w["last_updated"].isoformat()

    # Извлекаем только новые транзакции
    transactions = list(db.transactions.find(tx_filter))
    for tx in transactions:
        tx["_id"] = str(tx["_id"])
        if "value_raw" in tx:
            tx["value_raw"] = str(tx["value_raw"])
        if "timestamp" in tx and hasattr(tx["timestamp"], "isoformat"):
            tx["timestamp"] = tx["timestamp"].isoformat()
        if "fetched_at" in tx and hasattr(tx["fetched_at"], "isoformat"):
            tx["fetched_at"] = tx["fetched_at"].isoformat()

    # Сохраняем текущее время как время синхронизации
    current_sync_time = datetime.utcnow()
    sync_state.update_one(
        {"_id": "last_sync"}, {"$set": {"timestamp": current_sync_time}}, upsert=True
    )

    client.close()

    print(
        f"Extracted NEW data from MongoDB: {len(wallets)} wallets, {len(transactions)} transactions"
    )

    context["ti"].xcom_push(key="wallets", value=wallets)
    context["ti"].xcom_push(key="transactions", value=transactions)

    return {"wallets": len(wallets), "transactions": len(transactions)}


def load_to_postgres(**context):
    """
    Инкрементальная загрузка данных в PostgreSQL.
    Загружает только новые записи из MongoDB.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    postgres_uri = os.getenv(
        "POSTGRES_URI", "postgresql://postgres:postgres@postgres-dw:5432/blockchain"
    )

    wallets = context["ti"].xcom_pull(key="wallets", task_ids="extract_from_mongodb")
    transactions = context["ti"].xcom_pull(key="transactions", task_ids="extract_from_mongodb")

    wallets_loaded = 0
    transactions_loaded = 0

    # Если нет новых данных — пропускаем
    if not wallets and not transactions:
        print("ℹ️ No new data to load - skipping")
        return {"wallets_loaded": 0, "transactions_loaded": 0}

    conn = psycopg2.connect(postgres_uri)
    cur = conn.cursor()

    # Загружаем новые/обновленные кошельки
    if wallets:
        wallet_values = [
            (w["address"], w.get("transaction_count", 0), w.get("added_at"), w.get("last_updated"))
            for w in wallets
        ]

        execute_values(
            cur,
            """
            INSERT INTO wallets (address, transaction_count, added_at, last_updated)
            VALUES %s
            ON CONFLICT (address) DO UPDATE SET
                transaction_count = EXCLUDED.transaction_count,
                last_updated = EXCLUDED.last_updated,
                loaded_at = CURRENT_TIMESTAMP
            """,
            wallet_values,
        )
        wallets_loaded = len(wallets)
        print(f"✓ Loaded {wallets_loaded} NEW wallets to PostgreSQL")

    # Загружаем новые транзакции
    if transactions:
        tx_values = [
            (
                tx.get("hash"),
                tx.get("wallet_address"),
                tx.get("from_address"),
                tx.get("to_address"),
                tx.get("value_eth"),
                tx.get("gas_used"),
                tx.get("gas_price"),
                tx.get("block_number"),
                tx.get("is_error", False),
                tx.get("timestamp"),
            )
            for tx in transactions
        ]

        execute_values(
            cur,
            """
            INSERT INTO transactions (hash, wallet_address, from_address, to_address, value_eth, gas_used, gas_price, block_number, is_error, timestamp)
            VALUES %s
            ON CONFLICT (hash) DO UPDATE SET
                loaded_at = CURRENT_TIMESTAMP
            """,
            tx_values,
        )
        transactions_loaded = len(transactions)
        print(f"✓ Loaded {transactions_loaded} NEW transactions to PostgreSQL")

    conn.commit()
    cur.close()
    conn.close()

    return {"wallets_loaded": wallets_loaded, "transactions_loaded": transactions_loaded}


def print_statistics(**context):
    """Вывод статистики с информацией об инкрементальной загрузке"""
    import psycopg2

    postgres_uri = os.getenv(
        "POSTGRES_URI", "postgresql://postgres:postgres@postgres-dw:5432/blockchain"
    )
    conn = psycopg2.connect(postgres_uri)
    cur = conn.cursor()

    # Общее количество
    cur.execute("SELECT COUNT(*) FROM wallets")
    wallet_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM transactions")
    tx_count = cur.fetchone()[0]

    # Статистика по объему
    cur.execute(
        """
        SELECT
            COALESCE(ROUND(SUM(value_eth)::numeric, 4), 0) as total_volume,
            COALESCE(ROUND(AVG(value_eth)::numeric, 6), 0) as avg_value,
            COUNT(DISTINCT wallet_address) as unique_wallets
        FROM transactions
    """
    )
    stats = cur.fetchone()

    # Данные за последние 5 минут
    cur.execute(
        """
        SELECT COUNT(*) FROM transactions
        WHERE loaded_at > NOW() - INTERVAL '5 minutes'
    """
    )
    new_tx_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    # Получаем данные о загруженных записях из предыдущего таска
    load_result = context["ti"].xcom_pull(task_ids="load_to_postgres")

    print("\n" + "=" * 60)
    print("📊 BLOCKCHAIN DATA WAREHOUSE STATISTICS")
    print("=" * 60)
    print(f"🔐 Total Wallets:        {wallet_count}")
    print(f"📝 Total Transactions:   {tx_count}")
    print(f"💰 Total Volume:         {stats[0]}")
    print(f"📈 Avg Transaction:      {stats[1]}")
    print(f"👛 Unique Wallets in TX: {stats[2]}")
    print("-" * 60)
    print(f"🆕 New TX (last 5 min):  {new_tx_count}")
    if load_result:
        print(
            f"📥 Loaded this run:      {load_result.get('transactions_loaded', 0)} tx, {load_result.get('wallets_loaded', 0)} wallets"
        )
    print(f"⏰ Last Update:          {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 60 + "\n")


# DAG Definition
with DAG(
    "blockchain_etl_pipeline",
    default_args=default_args,
    description="Full ETL: Fetch Blockchain Data via Etherscan → MongoDB → PostgreSQL",
    schedule_interval="*/30 * * * *",  # Каждые 30 минут
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "blockchain", "ethereum", "mongodb", "postgresql"],
) as dag:

    # Task 1: Сбор данных с Etherscan
    fetch_data = PythonOperator(
        task_id="fetch_blockchain_data",
        python_callable=fetch_token_transfers,
    )

    # Task 2: Извлечение из MongoDB
    extract_data = PythonOperator(
        task_id="extract_from_mongodb",
        python_callable=extract_from_mongodb,
    )

    # Task 3: Загрузка в PostgreSQL
    load_data = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    # Task 4: Статистика
    statistics = PythonOperator(
        task_id="print_statistics",
        python_callable=print_statistics,
    )

    # Pipeline: fetch → extract → load → stats
    fetch_data >> extract_data >> load_data >> statistics
