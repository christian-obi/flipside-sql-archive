with rune_price as (
  SELECT
    hour :: date as date,
    avg(close) as price
  from
    thorchain.price.fact_prices_ohlc_hourly
  GROUP BY
    1
),
block_reward as (
  SELECT
    fbl.day as day,
    (fbl.liquidity_fee * rp.price) as liquidity_fee_usd,
    (fbl.block_rewards * rp.price) as block_rewards_usd,
    (fbl.bonding_earnings * rp.price) as bonding_earning_usd,
    (fbl.liquidity_earnings * rp.price) as liquidity_earnings_usd
  FROM
    thorchain.defi.fact_block_rewards fbl
    JOIN rune_price rp ON fbl.day = rp.date
)
select
  day,
  block_rewards_usd,
  bonding_earning_usd,
  liquidity_earnings_usd,
  liquidity_fee_usd
FROM
  block_reward
WHERE
  day >= current_date - 31
ORDER BY
  1
