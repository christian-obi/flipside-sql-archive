with total_pool_tvl as (
  SELECT
    day,
    total_value_pooled_usd as pool_tvl
  FROM
    thorchain.defi.fact_daily_tvl
  where
    day = current_date
),
total_bonded_tvl as (
  SELECT
    day,
    total_value_bonded_usd as bonded_tvl
  FROM
    thorchain.defi.fact_daily_tvl
  WHERE
    day = current_date
),
chain_tvl as (
  SELECT
    day,
    total_value_locked_usd as tvl
  FROM
    thorchain.defi.fact_daily_tvl
)
SELECT
  a.pool_tvl,
  b.bonded_tvl,
  c.tvl
from
  total_pool_tvl a
  JOIN total_bonded_tvl b on a.day = b.day
  JOIN chain_tvl c on b.day = c.day
