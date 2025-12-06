{{
    config(
        materialized='table',
        tags=['marts', 'summary', 'daily']
    )
}}

with daily_stats as (
    select * from {{ ref('int_daily_transactions') }}
)

select
    transaction_date,
    transaction_count,
    unique_wallets,
    unique_senders,
    unique_receivers,
    total_volume_eth,
    avg_transaction_value_eth,
    failed_transactions,
    (transaction_count - failed_transactions) as successful_transactions,
    round((failed_transactions::numeric / nullif(transaction_count, 0)) * 100, 2) as failure_rate_pct,
    total_gas_used,
    avg_gas_used,
    current_timestamp as dbt_updated_at
from daily_stats
