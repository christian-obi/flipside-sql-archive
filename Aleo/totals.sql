SELECT
  count(tx_id) as total_transactions,
  sum(fee) as total_fee
FROM
  aleo.core.fact_transactions
where
  tx_succeeded = 'TRUE'
