WITH filtered_logs AS (
  SELECT
    tx_hash,
    DATE_TRUNC('day', block_timestamp) AS day,
    origin_function_signature
  FROM
    core.core.ez_decoded_event_logs
  WHERE
    contract_address in (
      '0xb3a8f0f0da9ffc65318aa39e55079796093029ad',
      '0xf5fa1728babc3f8d2a617397fac2696c958c3409'
    )
    AND origin_function_signature in ('0x6a627842')
    and event_removed = 'FALSE'
    and tx_succeeded = 'TRUE'
),
total as(
  SELECT
    fl.day as day,
    COUNT(ft.tx_hash) AS tx_count,
    SUM(ft.value_precise) AS total_value_precise,
    -- sum(ft.value_precise) over (order by fl.day),
    -- sum(tx_count) over (order by fl.day)
  FROM
    filtered_logs fl
    JOIN core.core.fact_transactions ft ON fl.tx_hash = ft.tx_hash
    AND fl.origin_function_signature = ft.origin_function_signature
  where
    ft.tx_succeeded = 'TRUE'
  GROUP BY
    fl.day
  ORDER BY
    fl.day
),
core_price as (
  SELECT
    hour :: date as date,
    avg(price) as price
  from
    core.price.ez_prices_hourly
  WHERE
    symbol = 'CORE'
  GROUP BY
    1
)
SELECT
  tl.day as day,
  tl.tx_count as tx_count,
  tl.total_value_precise,
  sum(total_value_precise) over (
    order by
      day
  ) as cumulative_value,
  sum(tx_count) over (
    order by
      day
  ) as cumulative_transaction,
  (tl.total_value_precise * cp.price) as value_usd,
  sum(value_usd) over (
    order by
      day
  ) as cumulative_value_usd
FROM
  total tl
  join core_price cp on tl.day = cp.date
order by
  day desc
