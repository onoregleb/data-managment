{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'wallets']
    )
}}

{% set dimension_name = 'wallets' %}
-- Dimension table for {{ dimension_name }}

with wallets as (
    select * from {{ ref('ods_wallets') }}
),

wallet_activity as (
    select * from {{ ref('fct_wallet_activity') }}
)

select
    w.wallet_id,
    w.wallet_address,
    w.transaction_count,
    w.added_at,
    w.last_updated,
    w.loaded_at,
    wa.actual_tx_count,
    wa.total_sent_eth,
    wa.total_received_eth,
    wa.net_balance_eth,
    wa.is_active,
    wa.first_transaction_at,
    wa.last_transaction_at
from wallets w
left join wallet_activity wa on w.wallet_id = wa.wallet_id
