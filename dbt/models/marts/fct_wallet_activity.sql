{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'wallets']
    )
}}

with wallet_stats as (
    select * from {{ ref('int_wallet_transactions') }}
)

select
    wallet_id,
    wallet_address,
    expected_tx_count,
    actual_tx_count,
    total_sent_eth,
    total_received_eth,
    total_volume_eth,
    (total_received_eth - total_sent_eth) as net_balance_eth,
    first_transaction_at,
    last_transaction_at,
    case
        when actual_tx_count > 0 then true
        else false
    end as is_active,
    current_timestamp as dbt_updated_at
from wallet_stats
