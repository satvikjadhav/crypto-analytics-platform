{{ config(materialized='view', schema='STAGING') }}

SELECT
    symbol as coin_symbol,
    cast(price as float) as price,
    cast(quantity as float) as quantity,
    trade_time as trade_timestamp,
    is_buyer_maker,
    ingestion_ts,
    date as trade_date,
    DATE_TRUNC('day', trade_time) AS trade_date_trunc

FROM {{ source('raw', 'trades') }}