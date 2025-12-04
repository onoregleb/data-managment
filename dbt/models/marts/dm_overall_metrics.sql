-- DataMart: Общие метрики системы
-- Сводная аналитическая витрина

SELECT
    (SELECT COUNT(*) FROM {{ ref('stg_wallets') }}) as total_wallets,
    (SELECT COUNT(*) FROM {{ ref('stg_transactions') }}) as total_transactions,
    (SELECT COUNT(*) FROM {{ ref('stg_transactions') }} WHERE is_error) as failed_transactions,
    (SELECT COALESCE(SUM(value_eth), 0) FROM {{ ref('stg_transactions') }}) as total_volume_eth,
    (SELECT COALESCE(AVG(value_eth), 0) FROM {{ ref('stg_transactions') }}) as avg_transaction_eth,
    (SELECT COALESCE(MAX(value_eth), 0) FROM {{ ref('stg_transactions') }}) as max_transaction_eth,
    (SELECT COALESCE(SUM(gas_cost_eth), 0) FROM {{ ref('stg_transactions') }}) as total_gas_spent_eth,
    (SELECT COUNT(DISTINCT wallet_address) FROM {{ ref('stg_transactions') }}) as active_wallets,
    (SELECT MIN(tx_timestamp) FROM {{ ref('stg_transactions') }}) as first_transaction_date,
    (SELECT MAX(tx_timestamp) FROM {{ ref('stg_transactions') }}) as last_transaction_date,
    CURRENT_TIMESTAMP as updated_at

