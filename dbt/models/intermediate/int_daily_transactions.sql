{{
    config(
        materialized='view',
        tags=['intermediate', 'transactions', 'daily']
    )
}}

{% set granularity = 'daily' %}
-- Aggregating on {{ granularity }} basis

with transactions as (
    select * from {{ ref('ods_transactions') }}
),

daily_transactions as (
    select
        date(transaction_timestamp) as transaction_date,
        count(*) as transaction_count,
        count(distinct wallet_address) as unique_wallets,
        count(distinct from_address) as unique_senders,
        count(distinct to_address) as unique_receivers,
        sum(value_eth) as total_volume_eth,
        avg(value_eth) as avg_transaction_value_eth,
        sum(case when is_error then 1 else 0 end) as failed_transactions,
        sum(gas_used) as total_gas_used,
        avg(gas_used) as avg_gas_used
    from transactions
    where transaction_timestamp is not null
    group by date(transaction_timestamp)
)

select * from daily_transactions
order by transaction_date desc
