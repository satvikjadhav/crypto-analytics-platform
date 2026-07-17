{{ config(materialized='view', schema='STAGING') }}

WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY symbol, price, quantity, trade_time, date
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM {{ source('raw', 'trades') }}
)

SELECT
    left(symbol,3) as coin_symbol,
    cast(price as float) as price,
    cast(quantity as float) as quantity,
    trade_time as trade_timestamp,
    is_buyer_maker,
    ingestion_ts,
    date as trade_date,
    DATE_TRUNC('day', trade_time) AS trade_date_trunc
FROM deduped
WHERE rn = 1