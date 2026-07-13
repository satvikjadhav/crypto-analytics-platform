{{ config(materialized='view', schema='STAGING') }}

select
    t.coin_symbol,
    t.price,
    t.quantity,
    t.trade_timestamp,
    t.is_buyer_maker,
    t.ingestion_ts,
    t.price * t.quantity as trade_value_usd,
    m.coin_id,
    m.name as coin_name,
    m.market_cap,
    m.market_cap_rank,
    m.circulating_supply,
    m.current_price as latest_price,
    m.price_change_pct_24h,
    m.ingested_at as meta_ingested_at

from {{ ref('stg_trades') }} as t
left join {{ ref('stg_market_meta') }} as m
on UPPER(t.coin_symbol) = UPPER(m.coin_symbol)