-- Инициализация PostgreSQL для Blockchain Data Warehouse

-- Таблица кошельков
CREATE TABLE IF NOT EXISTS wallets (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) UNIQUE NOT NULL,
    transaction_count INTEGER DEFAULT 0,
    added_at TIMESTAMP,
    last_updated TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица транзакций
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
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_tx_wallet ON transactions (wallet_address);
CREATE INDEX IF NOT EXISTS idx_tx_from ON transactions (from_address);
CREATE INDEX IF NOT EXISTS idx_tx_to ON transactions (to_address);
CREATE INDEX IF NOT EXISTS idx_tx_block ON transactions (block_number);

-- View: статистика по кошелькам
CREATE OR REPLACE VIEW wallet_stats AS
SELECT
    w.address,
    w.transaction_count,
    COUNT(t.id) AS actual_tx_count,
    COALESCE(SUM(CASE WHEN t.from_address = w.address THEN t.value_eth ELSE 0 END), 0) AS total_sent_eth,
    COALESCE(SUM(CASE WHEN t.to_address = w.address THEN t.value_eth ELSE 0 END), 0) AS total_received_eth
FROM wallets AS w
LEFT JOIN transactions AS t ON w.address = t.wallet_address
GROUP BY w.address, w.transaction_count;

-- View: общая статистика
CREATE OR REPLACE VIEW overall_stats AS
SELECT
    (SELECT COUNT(*) FROM wallets) AS total_wallets,
    (SELECT COUNT(*) FROM transactions) AS total_transactions,
    (SELECT COALESCE(SUM(value_eth), 0) FROM transactions) AS total_volume_eth,
    (SELECT COALESCE(AVG(value_eth), 0) FROM transactions) AS avg_transaction_eth;
