SELECT
  date_trunc('day', block_timestamp) as tx_date,
  platform,
  count(DISTINCT tx_hash) as transaction,
  sum(transaction) over (
    order by
      tx_date
  ) as cumulative_tx
FROM
  near.defi.ez_bridge_activity
WHERE
  receipt_succeeded = TRUE
  and tx_date >= '{{start_date}}'
group BY
  platform,
  tx_date
order by
  1 DESC
