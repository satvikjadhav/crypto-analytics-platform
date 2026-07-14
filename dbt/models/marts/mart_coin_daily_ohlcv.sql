{{
    config(
        materialized='table',
        schema='MARTS',
        cluster_by=['coin_symbol', 'trade_date'],
    )
}}

with daily as (
    select
        trade_date,
        coin_symbol,
        coin_name,
        market_cap_rank,
        sum(quantity) as total_volume,
        sum(trade_value_usd) as total_volume_usd,
        round(
            sum(price * quantity) / nullif(sum(quantity), 0), 8
        ) as vwap,
        max(price) as high_price,
        min(price) as low_price,
        count(*) as trade_count
    from {{ ref('int_trades_enriched') }}
    group by 1,2,3,4
),

open_close as (
    select
        trade_date,
        coin_symbol,
        first_value(price) over (partition by trade_date, coin_symbol order by trade_timestamp asc) as open_price,
        first_value(price) over (partition by trade_date, coin_symbol order by trade_timestamp desc) as close_price
    from {{ ref('int_trades_enriched') }}
    -- qualify: filters after window functions are calculated.
    -- WHERE  → filters raw rows (before window functions)
    -- HAVING → filters after GROUP BY aggregations  
    -- QUALIFY → filters after window functions  ← this is what's used here
    qualify row_number() over (partition by trade_date, coin_symbol order by trade_timestamp) = 1

)

select
    d.trade_date,
    d.coin_symbol,
    d.coin_name,
    d.market_cap_rank,
    d.total_volume,
    d.total_volume_usd,
    d.vwap,
    d.high_price,
    d.low_price,
    d.trade_count,
    o.open_price,
    o.close_price,
    round(
        (o.close_price - o.open_price) / nullif(o.open_price, 0) * 100, 4
    ) as day_change_pct


from daily as d
join open_close as o on d.trade_date = o.trade_date and d.coin_symbol = o.coin_symbol