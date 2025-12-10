{{
    config(
        materialized='view',
        tags=['staging', 'transactions']
    )
}}

{% set table_name = 'transactions' %}
-- Model for {{ table_name }}

with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        id as transaction_id,
        hash as transaction_hash,
        lower(wallet_address) as wallet_address,
        lower(from_address) as from_address,
        lower(to_address) as to_address,
        value_eth,
        gas_used,
        gas_price,
        block_number,
        is_error,
        timestamp as transaction_timestamp,
        loaded_at
    from source
),

dedup as (
    select
        *,
        row_number() over (
            partition by transaction_hash
            order by coalesce(loaded_at, '1970-01-01'::timestamp) desc, transaction_id desc
        ) as rn
    from renamed
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
    loaded_at
from dedup
where rn = 1
