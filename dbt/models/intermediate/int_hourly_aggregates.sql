{{ config(materialized='view', schema='STAGING') }}

select
    date_trunc("hour", trade_timestamp) as hour_ts,
    coin_symbol,
    round(
        sum(price * quantity) / nullif(sum(quantity), 0), 8
    ) as vwap,
    sum(quantity) as total_volume,
    sum(price * quantity) as total_volume_usd,
    count(*) as trade_count,
    max(price) as high_price,
    min(price) as low_price
from {{ ref('stg_trades') }}
group by 1,2