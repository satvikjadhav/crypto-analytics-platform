{{ config(materialized='table', schema='MARTS') }}

with total_market as (
    select sum(market_cap) as total_market_cap
    from {{ ref('stg_market_meta') }}
    where market_cap > 0
)

select
    o.trade_date,
    o.coin_symbol,
    o.coin_name,
    o.market_cap_rank,
    o.close_price as current_price,
    m.market_cap,
    o.total_volume_usd as volume_24h_usd,
    o.day_change_pct as price_change_24h_pct,
    m.circulating_supply,
    round(m.market_cap / nullif(t.total_market_cap, 0) * 100, 4) as market_dominance_pct,
    m.ath,
    round((o.close_price - m.ath) / nullif(m.ath, 0) * 100, 2) as pct_from_ath,
    t.total_market_cap



from {{ ref('mart_coin_daily_ohlcv') }}  as o
join {{ ref('stg_market_meta') }} as m on upper(o.coin_symbol) = upper(m.symbol)
cross join total_market as t
where o.trade_date = current_date()
order by o.market_cap_rank asc nulls last
