SELECT
  date_trunc('week', block_timestamp) as date,
  count(*) as total_transaction,
  sum(total_transaction) over (
    order by
      date
  ) as cumulative_transaction_activity
FROM
  core.core.fact_transactions
WHERE
  date >= '2024-01-23'
GROUP by
  1
order by
  1 desc 
--staking address = 0x0000000000000000000000000000000000001007
