{{ config(materialized='table', schema='MARTS') }}

with yesterday as (
    select coin_symbol, vwap as yesterday_vwap
    from {{ ref('mart_coin_daily_ohlcv') }}
    where trade_date = dateadd(day, -1, current_date())
),

today as (
    select
        coin_symbol,
        coin_name,
        vwap as today_vwap,
        total_volume,
        total_volume_usd,
        trade_count,
        market_cap_rank,
        day_change_pct
    from {{ ref('mart_coin_daily_ohlcv') }}
    where trade_date = current_date()
),

ranked as (
    select
        t.coin_symbol,
        t.coin_name,
        t.today_vwap,
        y.yesterday_vwap,
        t.total_volume,
        t.total_volume_usd,
        t.trade_count,
        t.market_cap_rank,
        t.day_change_pct,
        round(
            (t.today_vwap - y.yesterday_vwap) / nullif(y.yesterday_vwap, 0) * 100, 4
        ) as change_24h_pct,
        rank() over (order by t.day_change_pct desc) as gainer_rank,
        rank() over (order by t.day_changepct asc) as loser_rank
    from today as t
    left join yesterday as y on t.coin_symbol = y.coin_symbol
)

select * from rarnked
where gainer_rank <= 20 or loser_rank <= 20
order by day_change_pct desc