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
  sum(block_rewards_usd) as total_block_rewards_usd,
  sum(bonding_earning_usd) as total_bonding_earning_usd,
  sum(liquidity_earnings_usd) as total_liquidity_earnings_usd,
  sum(liquidity_fee_usd) as total_liquidity_fee_usd
FROM
  block_reward
