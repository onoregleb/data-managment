"""
EL Pipeline: MongoDB → PostgreSQL

Переносит данные о блокчейн-транзакциях:
1. Извлекает из MongoDB (blockchain_raw)
2. Загружает в PostgreSQL (blockchain)
"""

import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def extract_wallets(**context):
    """Извлечение кошельков из MongoDB"""
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")
    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    wallets = list(db.wallets.find())
    print(f"Extracted {len(wallets)} wallets from MongoDB")

    for w in wallets:
        w["_id"] = str(w["_id"])
        if "added_at" in w:
            w["added_at"] = w["added_at"].isoformat()
        if "last_updated" in w:
            w["last_updated"] = w["last_updated"].isoformat()

    client.close()
    context["ti"].xcom_push(key="wallets", value=wallets)
    return len(wallets)


def extract_transactions(**context):
    """Извлечение транзакций из MongoDB"""
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")
    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    transactions = list(db.transactions.find())
    print(f"Extracted {len(transactions)} transactions from MongoDB")

    for tx in transactions:
        tx["_id"] = str(tx["_id"])
        if "timestamp" in tx:
            tx["timestamp"] = tx["timestamp"].isoformat()
        if "fetched_at" in tx:
            tx["fetched_at"] = tx["fetched_at"].isoformat()

    client.close()
    context["ti"].xcom_push(key="transactions", value=transactions)
    return len(transactions)


def load_wallets(**context):
    """Загрузка кошельков в PostgreSQL"""
    import psycopg2
    from psycopg2.extras import execute_values

    postgres_uri = os.getenv(
        "POSTGRES_URI", "postgresql://postgres:postgres@postgres-dw:5432/blockchain"
    )
    wallets = context["ti"].xcom_pull(key="wallets", task_ids="extract_wallets")

    if not wallets:
        print("No wallets to load")
        return 0

    conn = psycopg2.connect(postgres_uri)
    cur = conn.cursor()

    # Создаем таблицу
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS wallets (
            id SERIAL PRIMARY KEY,
            address VARCHAR(42) UNIQUE NOT NULL,
            transaction_count INTEGER DEFAULT 0,
            added_at TIMESTAMP,
            last_updated TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    values = [
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
        values,
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(wallets)} wallets to PostgreSQL")
    return len(wallets)


def load_transactions(**context):
    """Загрузка транзакций в PostgreSQL"""
    import psycopg2
    from psycopg2.extras import execute_values

    postgres_uri = os.getenv(
        "POSTGRES_URI", "postgresql://postgres:postgres@postgres-dw:5432/blockchain"
    )
    transactions = context["ti"].xcom_pull(key="transactions", task_ids="extract_transactions")

    if not transactions:
        print("No transactions to load")
        return 0

    conn = psycopg2.connect(postgres_uri)
    cur = conn.cursor()

    # Создаем таблицу транзакций
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            hash VARCHAR(66) UNIQUE NOT NULL,
            wallet_address VARCHAR(42),
            from_address VARCHAR(42),
            to_address VARCHAR(42),
            value_eth DECIMAL(30, 18),
            gas_used BIGINT,
            gas_price BIGINT,
            block_number BIGINT,
            is_error BOOLEAN DEFAULT FALSE,
            timestamp TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    values = []
    for tx in transactions:
        values.append(
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
        )

    execute_values(
        cur,
        """
        INSERT INTO transactions (hash, wallet_address, from_address, to_address, value_eth, gas_used, gas_price, block_number, is_error, timestamp)
        VALUES %s
        ON CONFLICT (hash) DO UPDATE SET
            loaded_at = CURRENT_TIMESTAMP
        """,
        values,
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(transactions)} transactions to PostgreSQL")
    return len(transactions)


def log_stats(**context):
    """Логирование статистики"""
    import psycopg2

    postgres_uri = os.getenv(
        "POSTGRES_URI", "postgresql://postgres:postgres@postgres-dw:5432/blockchain"
    )
    conn = psycopg2.connect(postgres_uri)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM wallets")
    wallet_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM transactions")
    tx_count = cur.fetchone()[0]

    cur.execute(
        """
        SELECT
            ROUND(SUM(value_eth)::numeric, 4) as total_volume,
            ROUND(AVG(value_eth)::numeric, 4) as avg_value,
            COUNT(DISTINCT wallet_address) as unique_wallets
        FROM transactions
    """
    )
    stats = cur.fetchone()

    print("=" * 50)
    print("📊 BLOCKCHAIN DATA STATISTICS")
    print("=" * 50)
    print(f"Wallets: {wallet_count}")
    print(f"Transactions: {tx_count}")
    print(f"Total Volume: {stats[0]} ETH")
    print(f"Avg Transaction: {stats[1]} ETH")
    print(f"Unique Wallets in TX: {stats[2]}")
    print("=" * 50)

    cur.close()
    conn.close()


# DAG
with DAG(
    "el_mongo_to_postgres",
    default_args=default_args,
    description="EL: MongoDB → PostgreSQL (Blockchain Data)",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["el", "blockchain", "mongodb", "postgresql"],
) as dag:

    extract_wallets_task = PythonOperator(
        task_id="extract_wallets",
        python_callable=extract_wallets,
    )

    extract_transactions_task = PythonOperator(
        task_id="extract_transactions",
        python_callable=extract_transactions,
    )

    load_wallets_task = PythonOperator(
        task_id="load_wallets",
        python_callable=load_wallets,
    )

    load_transactions_task = PythonOperator(
        task_id="load_transactions",
        python_callable=load_transactions,
    )

    stats_task = PythonOperator(
        task_id="log_statistics",
        python_callable=log_stats,
    )

    # Pipeline
    (
        [extract_wallets_task, extract_transactions_task]
        >> load_wallets_task
        >> load_transactions_task
        >> stats_task
    )
