"""
EL Pipeline: MongoDB → PostgreSQL

Переносит данные о блокчейн-транзакциях:
1. Извлекает из MongoDB (blockchain_raw)
2. Загружает в PostgreSQL (blockchain)
"""

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Serialize dbt runs across DAGs (LocalExecutor may run tasks concurrently).
DBT_LOCK_FILE = os.getenv("DBT_LOCK_FILE", "/tmp/dbt_global.lock")


def extract_wallets(**context):
    """Извлечение кошельков из MongoDB (только метрики, без передачи данных через XCom)."""
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")
    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    # Не читаем все документы в память (это может убить таск по OOM).
    count = db.wallets.estimated_document_count()
    print(f"Found {count} wallets in MongoDB")

    client.close()
    return int(count)


def extract_transactions(**context):
    """Извлечение транзакций из MongoDB (только метрики, без передачи данных через XCom)."""
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")
    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    # Не читаем все документы в память (это может убить таск по OOM).
    count = db.transactions.estimated_document_count()
    print(f"Found {count} transactions in MongoDB")

    client.close()
    return int(count)


def load_wallets(**context):
    """Загрузка кошельков в PostgreSQL"""
    import psycopg2
    from psycopg2.extras import execute_values
    from pymongo import MongoClient

    postgres_uri = os.getenv(
        "POSTGRES_URI", "postgresql://postgres:postgres@postgres-dw:5432/blockchain"
    )
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")

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

    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    values = []
    for w in db.wallets.find(
        {},
        projection={
            "_id": 0,
            "address": 1,
            "transaction_count": 1,
            "added_at": 1,
            "last_updated": 1,
        },
    ).batch_size(2000):
        addr = w.get("address")
        if not addr:
            continue
        values.append(
            (addr, w.get("transaction_count", 0), w.get("added_at"), w.get("last_updated"))
        )

    client.close()

    if not values:
        print("No wallets to load")
        cur.close()
        conn.close()
        return 0

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

    print(f"Loaded {len(values)} wallets to PostgreSQL")
    return len(values)


def load_transactions(**context):
    """Загрузка транзакций в PostgreSQL"""
    from decimal import Decimal

    import psycopg2
    from bson.decimal128 import Decimal128
    from psycopg2.extras import execute_values
    from pymongo import MongoClient

    postgres_uri = os.getenv(
        "POSTGRES_URI", "postgresql://postgres:postgres@postgres-dw:5432/blockchain"
    )
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:mongo@mongodb:27017/")

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
            value_eth DECIMAL(38, 18),
            gas_used BIGINT,
            gas_price BIGINT,
            block_number BIGINT,
            is_error BOOLEAN DEFAULT FALSE,
            timestamp TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    def sanitize(val):
        if isinstance(val, Decimal128):
            return float(val.to_decimal())
        if isinstance(val, Decimal):
            return float(val)
        return val

    insert_sql = """
        INSERT INTO transactions (
            hash, wallet_address, from_address, to_address, value_eth,
            gas_used, gas_price, block_number, is_error, timestamp
        )
        VALUES %s
        ON CONFLICT (hash) DO UPDATE SET
            loaded_at = CURRENT_TIMESTAMP
    """

    batch_size = int(os.getenv("TX_BATCH_SIZE", "5000"))
    total = 0

    client = MongoClient(mongo_uri)
    db = client["blockchain_raw"]

    cursor = db.transactions.find(
        {},
        projection={
            "_id": 0,
            "hash": 1,
            "wallet_address": 1,
            "from_address": 1,
            "to_address": 1,
            "value_eth": 1,
            "gas_used": 1,
            "gas_price": 1,
            "block_number": 1,
            "is_error": 1,
            "timestamp": 1,
        },
    ).batch_size(batch_size)

    values = []
    for tx in cursor:
        tx_hash = tx.get("hash")
        if not tx_hash:
            continue
        values.append(
            (
                tx_hash,
                tx.get("wallet_address"),
                tx.get("from_address"),
                tx.get("to_address"),
                sanitize(tx.get("value_eth")),
                tx.get("gas_used"),
                sanitize(tx.get("gas_price")),
                tx.get("block_number"),
                tx.get("is_error", False),
                tx.get("timestamp"),
            )
        )

        if len(values) >= batch_size:
            execute_values(cur, insert_sql, values, page_size=batch_size)
            conn.commit()
            total += len(values)
            values.clear()
            print(f"Loaded {total} transactions so far...")

    if values:
        execute_values(cur, insert_sql, values, page_size=min(batch_size, len(values)))
        conn.commit()
        total += len(values)

    client.close()

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {total} transactions to PostgreSQL")
    return total


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
    schedule_interval="*/30 * * * *",
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

    # DBT tasks
    DBT_PROJECT_DIR = "/opt/airflow/dbt"
    DBT_PROFILES_DIR = "/opt/airflow/dbt"

    # Очистка dbt backup таблиц перед запуском
    dbt_cleanup = BashOperator(
        task_id="dbt_cleanup",
        bash_command=(
            f'flock -w 1800 {DBT_LOCK_FILE} bash -c "'
            "PGPASSWORD=\\$POSTGRES_PASSWORD psql -h \\$POSTGRES_HOST -p \\$POSTGRES_PORT -U \\$POSTGRES_USER -d \\$POSTGRES_DB "
            '-c \\"DO \\\\\\$\\\\\\$ DECLARE r record; BEGIN '
            "FOR r IN ("
            "  SELECT n.nspname AS schemaname, c.relname AS name, c.relkind "
            "  FROM pg_catalog.pg_class c "
            "  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "  WHERE c.relname LIKE '%__dbt_backup'"
            ") LOOP "
            "  IF r.relkind = 'v' THEN "
            "    EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', r.schemaname, r.name); "
            "  ELSIF r.relkind = 'm' THEN "
            "    EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %I.%I CASCADE', r.schemaname, r.name); "
            "  ELSE "
            "    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', r.schemaname, r.name); "
            "  END IF; "
            "END LOOP; "
            'END \\\\\\$\\\\\\$;\\"'
            '"'
        ),
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres-dw"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "blockchain"),
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "export PATH=$PATH:/home/airflow/.local/bin && "
            "export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && "
            f"flock -w 1800 {DBT_LOCK_FILE} bash -c "
            f'"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod"'
        ),
        env={
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres-dw"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "blockchain"),
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "export PATH=$PATH:/home/airflow/.local/bin && "
            "export PYTHONPATH=$PYTHONPATH:/home/airflow/.local/lib/python3.11/site-packages && "
            f"flock -w 1800 {DBT_LOCK_FILE} bash -c "
            f'"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod"'
        ),
        env={
            "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres-dw"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "blockchain"),
        },
    )

    # Pipeline: EL -> Stats -> DBT Cleanup -> DBT Run -> DBT Test
    (
        [extract_wallets_task, extract_transactions_task]
        >> load_wallets_task
        >> load_transactions_task
        >> stats_task
        >> dbt_cleanup
        >> dbt_run
        >> dbt_test
    )
