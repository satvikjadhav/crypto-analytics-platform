{{ config(materialized='view', schema="STAGING") }}

SELECT
    coin_id,
    UPPER(symbol)                               AS symbol,
    name,
    current_price,
    CAST(market_cap AS NUMBER(20,0))            AS market_cap,
    market_cap_rank,
    total_volume,
    price_change_24h,
    price_change_pct_24h,
    circulating_supply,
    ath,
    TO_TIMESTAMP_NTZ(ingestion_ts / 1000)       AS ingested_at

FROM {{ source('raw', 'market_meta') }}