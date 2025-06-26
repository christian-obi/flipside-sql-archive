with daily_user as(
  SELECT
    date_trunc('day', block_timestamp) as day,
    count(tx_id) as trans,
    --token_contract,
    sum(amount) as amount,
    --amount_daily_sum
    --tx_succeeded
  FROM
    flow.core.ez_token_transfers
  WHERE
    tx_succeeded = True
    AND day >= '2024-01-01'
  GROUP BY
    1
  ORDER BY
    1
),
total_trans as (
  SELECT
    sum(trans) as total_transaction
  FROM
    daily_user
) --percentage as(
SELECT
  daily.day,
  daily.amount / pow(10, 7) as amount_in_mil,
  (
    daily.trans /(
      SELECT
        total_transaction
      FROM
        total_trans
    )
  ) * 100 AS percentage_trans,
  CASE
    when daily.day >= '2024-01-01'
    AND daily.day < '2024-02-01' then 'january'
    when daily.day >= '2024-02-01'
    and daily.day < '2024-03-01' then 'february'
    when daily.day >= '2024-03-01'
    AND daily.day < '2024-04-01' then 'march'
    when daily.day >= '2024-04-01'
    AND daily.day < '2024-05-01' then 'april'
    when daily.day >= '2024-05-01'
    AND daily.day < '2024-06-01' then 'may'
    when daily.day >= '2024-06-01'
    AND daily.day < '2024-07-01' then 'june'
    when daily.day >= '2024-07-01'
    AND daily.day < '2024-08-01' then 'july'
    when daily.day >= '2024-08-01'
    AND daily.day < '2024-09-01' then 'august'
    when daily.day >= '2024-09-01'
    AND daily.day < '2024-10-01' then 'september'
    when daily.day >= '2024-10-01'
    AND daily.day < '2024-11-01' then 'october'
    when daily.day >= '2024-11-01'
    AND daily.day < '2024-12-01' then 'november'
    when daily.day >= '2024-12-01'
    and daily.day < '2025-01-01' then 'december'
    else NULL
  end as month
FROM
  daily_user daily
GROUP BY
  1,
  2,
  3
ORDER BY
  1 --),
  --total
