SELECT
  tx_type as trans_type,
  count(tx_id) as transactions,
  sum(fee) as fee
from
  aleo.core.fact_transactions
where
  tx_succeeded = 'TRUE'
group BY
  tx_type
