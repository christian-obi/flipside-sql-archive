with avg_transaction_fee_core as (
  SELECT
    DATE_TRUNC('month', block_timestamp) AS month,
    AVG(tx_fee_precise) AS avg_transaction_fee
  FROM
    core.core.fact_transactions
  GROUP BY
    month
  ORDER BY
    month
),
usd_price as (
  SELECT
    date_trunc('month', HOUR) as date,
    avg(price) as price,
  FROM
    core.price.ez_prices_hourly
  where
    symbol like 'CORE'
    and date >= '2024-01-01'
  GROUP by
    1
)
SELECT
  fee_core.month as month,
  fee_core.avg_transaction_fee * p.price as avg_transaction_fee_usd,
  fee_core.avg_transaction_fee
FROM
  avg_transaction_fee_core fee_core
  JOIN usd_price p on fee_core.month = p.date
ORDER BY
  1 DESC
