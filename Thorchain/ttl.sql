with structure as (
  SELECT
    tx_id,
    block_timestamp,
    nvl(from_amount_usd, to_amount_usd) as amount_usd,
    from_address as user
  FROM
    thorchain.defi.fact_swaps
)
SELECT
  count(DISTINCT tx_id) as transactions,
  count(DISTINCT user) as users,
  sum(amount_usd) as total_volume_usd,
  avg(amount_usd) as average_amount_usd,
  transactions / count(DISTINCT block_timestamp :: date) as daily_average_transactions,
  users / count(DISTINCT block_timestamp :: date) as daily_average_users
FROM
  structure
