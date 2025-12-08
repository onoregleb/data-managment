{{
    config(
        materialized='view',
        tags=['intermediate', 'wallets', 'transactions']
    )
}}

{% set model_description = 'Aggregates wallet transaction stats' %}
-- {{ model_description }}

with wallets as (
    select * from {{ ref('stg_wallets') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

wallet_transactions as (
    select
        w.wallet_id,
        w.wallet_address,
        w.transaction_count as expected_tx_count,
        count(t.transaction_id) as actual_tx_count,
        sum(case when t.from_address = w.wallet_address then t.value_eth else 0 end) as total_sent_eth,
        sum(case when t.to_address = w.wallet_address then t.value_eth else 0 end) as total_received_eth,
        sum(t.value_eth) as total_volume_eth,
        min(t.transaction_timestamp) as first_transaction_at,
        max(t.transaction_timestamp) as last_transaction_at
    from wallets w
    left join transactions t on w.wallet_address = t.wallet_address
    group by w.wallet_id, w.wallet_address, w.transaction_count
)

select * from wallet_transactions
