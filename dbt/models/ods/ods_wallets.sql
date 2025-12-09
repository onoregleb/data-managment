{{ config(
    materialized='view',
    schema='ods',
    tags=['ods', 'wallets']
) }}

-- ODS wallets: validated and ready for downstream consumption
with src as (
    select * from {{ ref('stg_wallets') }}
),

validated as (
    select
        wallet_id,
        wallet_address,
        transaction_count,
        added_at,
        last_updated,
        loaded_at,
        {{ ethereum_address('wallet_address') }} as validated_wallet_address
    from src
)

select
    wallet_id,
    coalesce(validated_wallet_address, wallet_address) as wallet_address,
    transaction_count,
    added_at,
    last_updated,
    loaded_at
from validated
