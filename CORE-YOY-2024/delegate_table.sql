WITH delegate AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS date,
    COUNT(tx_hash) AS delegate_transactions,
    SUM(
      utils.udf_hex_to_int(SUBSTR(data, 3, 64)) / POW(10, 18)
    ) AS delegate_core_amount
  FROM
    core.core.fact_event_logs
  WHERE
    topic_0 = LOWER(
      '0x69e36aaf9558a3c30415c0a2bc6cb4c2d592c041a0718697bf69c2e7c7e0bdac'
    )
    AND contract_address IN (
      '0xf5fa1728babc3f8d2a617397fac2696c958c3409',
      '0x0000000000000000000000000000000000001007'
    )
    AND tx_succeeded
  GROUP BY
    1
  ORDER BY
    1
),
core_price AS (
  SELECT
    hour :: DATE AS day,
    AVG(price) AS price
  FROM
    core.price.ez_prices_hourly
  WHERE
    symbol = 'CORE'
  GROUP BY
    1
)
SELECT
  d.date,
  d.delegate_transactions,
  d.delegate_core_amount,
  SUM(d.delegate_core_amount) OVER (
    ORDER BY
      d.date
  ) AS cumulative_delegated_core,
  COALESCE(d.delegate_core_amount * cp.price, 0) AS delegated_core_usd,
  SUM(COALESCE(d.delegate_core_amount * cp.price, 0)) OVER (
    ORDER BY
      d.date
  ) AS cumulative_delegate_usd
FROM
  delegate d
  JOIN core_price cp ON d.date = cp.day
where
  d.date >= '2024-01-01'
ORDER BY
  d.date DESC
