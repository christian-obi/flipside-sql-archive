with rune_fee as (
  SELECT
    block_timestamp :: date as daily,
    sum(rune_e8 / pow(10, 8)) as daily_rune_fee
  FROM
    thorchain.defi.fact_gas_events
  GROUP BY
    1 --where block_timestamp::date = current_date 
),
rune_price as (
  SELECT
    hour :: date as date,
    avg(close) as price
  from
    thorchain.price.fact_prices_ohlc_hourly
  GROUP BY
    1
),
calculated_fee as (
  SELECT
    rf.daily as day,
    rf.daily_rune_fee as rune_fee_day,
    rp.price,
    (rf.daily_rune_fee * rp.price) as amount_usd
  FROM
    rune_fee rf
    JOIN rune_price rp ON rf.daily = rp.date
)
select
  sum(rune_fee_day) as total_rune_fee,
  sum(amount_usd) as total_amount
from
  calculated_fee
