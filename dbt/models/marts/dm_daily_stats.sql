-- DataMart: Ежедневная статистика
-- Аналитическая витрина с дневными агрегатами

SELECT
    DATE(tx_timestamp) as date,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT wallet_address) as unique_wallets,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    SUM(value_eth) as total_volume_eth,
    AVG(value_eth) as avg_tx_value_eth,
    MAX(value_eth) as max_tx_value_eth,
    SUM(gas_cost_eth) as total_gas_eth,
    AVG(gas_cost_eth) as avg_gas_eth,
    COUNT(CASE WHEN is_error THEN 1 END) as failed_transactions,
    ROUND(100.0 * COUNT(CASE WHEN is_error THEN 1 END) / NULLIF(COUNT(*), 0), 2) as error_rate_pct,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('stg_transactions') }}
WHERE tx_timestamp IS NOT NULL
GROUP BY DATE(tx_timestamp)
ORDER BY date DESC

