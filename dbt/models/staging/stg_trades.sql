{{ config(materialized='view', schema='STAGING') }}

SELECT
    symbol as coin_symbol,
    cast(price as float) as price,
    cast(quantity as float) as quantity,
    TO_TIMESTAMP_NTZ(trade_time / 1000) as trade_timestamp,
    is_buyer_maker,
    TO_TIMESTAMP_NTZ(ingestion_ts / 1000) as ingestion_ts,
    date as trade_date,
    DATE_TRUNC('day', TO_TIMESTAMP_NTZ(trade_time / 1000)) AS trade_date_trunc

FROM {{ source('raw', 'trades') }}