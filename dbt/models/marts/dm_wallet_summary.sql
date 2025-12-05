-- DataMart: Сводка по кошелькам
-- Аналитическая витрина с метриками активности кошельков

WITH wallet_transactions AS (
    SELECT
        wallet_address,
        COUNT(*) as total_tx,
        COUNT(CASE WHEN from_address = wallet_address THEN 1 END) as sent_tx,
        COUNT(CASE WHEN to_address = wallet_address THEN 1 END) as received_tx,
        SUM(CASE WHEN from_address = wallet_address THEN value_eth ELSE 0 END) as total_sent_eth,
        SUM(CASE WHEN to_address = wallet_address THEN value_eth ELSE 0 END) as total_received_eth,
        SUM(gas_cost_eth) as total_gas_spent_eth,
        MIN(tx_timestamp) as first_tx_date,
        MAX(tx_timestamp) as last_tx_date,
        COUNT(DISTINCT DATE(tx_timestamp)) as active_days
    FROM {{ ref('stg_transactions') }}
    WHERE NOT is_error
    GROUP BY wallet_address
)

SELECT
    w.wallet_address,
    w.transaction_count as reported_tx_count,
    COALESCE(t.total_tx, 0) as actual_tx_count,
    COALESCE(t.sent_tx, 0) as sent_tx_count,
    COALESCE(t.received_tx, 0) as received_tx_count,
    COALESCE(t.total_sent_eth, 0) as total_sent_eth,
    COALESCE(t.total_received_eth, 0) as total_received_eth,
    COALESCE(t.total_received_eth, 0) - COALESCE(t.total_sent_eth, 0) as net_flow_eth,
    COALESCE(t.total_gas_spent_eth, 0) as total_gas_spent_eth,
    t.first_tx_date,
    t.last_tx_date,
    COALESCE(t.active_days, 0) as active_days,
    CASE
        WHEN t.total_tx >= 100 THEN 'high'
        WHEN t.total_tx >= 10 THEN 'medium'
        ELSE 'low'
    END as activity_level,
    w.added_at,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('stg_wallets') }} w
LEFT JOIN wallet_transactions t ON w.wallet_address = t.wallet_address
