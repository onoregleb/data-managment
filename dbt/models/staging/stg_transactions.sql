-- Staging: очистка и стандартизация данных транзакций

SELECT
    id,
    hash as tx_hash,
    LOWER(wallet_address) as wallet_address,
    LOWER(from_address) as from_address,
    LOWER(to_address) as to_address,
    COALESCE(value_eth, 0) as value_eth,
    COALESCE(gas_used, 0) as gas_used,
    COALESCE(gas_price, 0) as gas_price,
    (COALESCE(gas_used, 0) * COALESCE(gas_price, 0)) / 1e18 as gas_cost_eth,
    block_number,
    COALESCE(is_error, false) as is_error,
    timestamp as tx_timestamp,
    loaded_at
FROM {{ source('raw', 'transactions') }}
WHERE hash IS NOT NULL

