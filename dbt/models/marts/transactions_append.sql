{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['marts', 'incremental', 'append', 'transactions']
    )
}}

{% set incremental_type = 'append' %}
-- Incremental type: {{ incremental_type }}
-- Appends only NEW transactions based on monotonically increasing `transaction_id` from raw Postgres.

with src as (
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
        loaded_at
    from {{ ref('stg_transactions') }}
)

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
    current_timestamp as dbt_updated_at
from src

{% if is_incremental() %}
  where transaction_id > (select coalesce(max(transaction_id), 0) from {{ this }})
{% endif %}
