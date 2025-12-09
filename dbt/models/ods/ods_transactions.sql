{{ config(
    materialized='view',
    schema='ods',
    tags=['ods', 'transactions']
) }}

-- ODS transactions with window metrics for downstream marts
with src as (
    select * from {{ ref('stg_transactions') }}
),

with_windows as (
    select
        transaction_id,
        transaction_hash,
        wallet_address,
        from_address,
        to_address,
        value_eth,
        gas_used,
        gas_price,
        block_number,
        is_error,
        transaction_timestamp,
        loaded_at,
        -- Window functions: cumulative volume and latest lag per wallet
        sum(value_eth) over (
            partition by wallet_address
            order by transaction_timestamp
            rows between unbounded preceding and current row
        ) as cumulative_volume_eth,
        row_number() over (
            partition by wallet_address
            order by transaction_timestamp desc
        ) as txn_rank_desc,
        lag(transaction_timestamp) over (
            partition by wallet_address
            order by transaction_timestamp
        ) as prev_tx_timestamp
    from src
)

select * from with_windows
