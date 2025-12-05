-- Staging: очистка и стандартизация данных кошельков

SELECT
    id,
    LOWER(address) as wallet_address,
    COALESCE(transaction_count, 0) as transaction_count,
    added_at,
    last_updated,
    loaded_at
FROM {{ source('raw', 'wallets') }}
WHERE address IS NOT NULL
