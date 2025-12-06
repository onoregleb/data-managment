{{
    config(
        materialized='view',
        tags=['staging', 'wallets']
    )
}}

with source as (
    select * from {{ source('raw', 'wallets') }}
),

renamed as (
    select
        id as wallet_id,
        address as wallet_address,
        transaction_count,
        added_at,
        last_updated,
        loaded_at
    from source
)

select * from renamed
