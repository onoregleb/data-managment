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
        wallet_address,
        from_address,
        to_address,
        value_eth,
        gas_used,
        gas_price,
        block_number,
        is_error,
        timestamp as transaction_timestamp,
        loaded_at
    from source
)

select * from renamed
