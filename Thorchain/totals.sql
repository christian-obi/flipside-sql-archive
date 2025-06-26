with structure as (
  SELECT
    tx_id,
    block_timestamp :: date as day,
    to_pool_address,
    nvl(from_amount_usd, to_amount_usd) as amount_usd,
    from_address as user
  FROM
    thorchain.defi.fact_swaps
)
SELECT
  day,
  count(DISTINCT tx_id) as transactions,
  sum(amount_usd) as total_volume_usd
FROM
  structure
WHERE
  day >= current_date - 31
GROUP by
  1
ORDER by
  1
