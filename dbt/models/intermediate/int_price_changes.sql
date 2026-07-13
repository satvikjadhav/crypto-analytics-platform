{{ config(materialized='view', schema='STAGING') }}

with lagged as (
    select
        coin_symbol,
        price,
        trade_timestamp,
        trade_date,
        lag(price) over (partition by coin_symbol order by trade_timestamp) as prev_price
    from {{ ref('stg_trades') }}
)

select
    coin_symbol,
    price,
    prev_price,
    trade_timestamp,
    trade_date,
    case
        when prev_price is null or prev_price = 0 then null
        else round((price - prev_price) / prev_price * 100, 6)
    end as price_change_pct,
    case
        when prev_price is null then null
        else price - prev_price
    end as price_change_abs

from lagged