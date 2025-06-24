SELECT
  date_trunc({{period_type}}, block_timestamp) as date,
  count(*) as transactions,
  lag(transactions) over (
    order by
      date
  ) as lag_transactions,
  round(
    100 *(transactions - lag_transactions) / lag_transactions,
    2
  ) as "%change in txs",
  count(DISTINCT from_address) as users,
  lag(users) over (
    order by
      date
  ) as lag_users,
  round(100 *(users - lag_users) / lag_users, 2) as "%change in users",
  sum(tx_fee_precise) as tx_fees,
  lag(tx_fees) over (
    order by
      date
  ) as lag_tx_fees,
  round(100 *(tx_fees - lag_tx_fees) / lag_tx_fees, 2) as "%change tx fees"
FROM
  core.core.fact_transactions
where
  tx_succeeded = 'TRUE'
  AND block_timestamp :: date >= '2024-01-23'
  and block_timestamp :: date < date_trunc({{period_type}}, current_date())
GROUP BY
  1
ORDER by
  1 DESC
