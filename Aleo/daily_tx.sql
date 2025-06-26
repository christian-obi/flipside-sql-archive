SELECT
  block_timestamp :: date as date,
  tx_type as trans_type,
  count(tx_id) as trans_count,
  sum(fee) as fee
FROM
  aleo.core.fact_transactions
where
  tx_succeeded = 'TRUE'
group by
  1,
  2
ORDER BY
  1
