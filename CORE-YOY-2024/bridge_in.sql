with filtered_logs as(
  SELECT
    date_trunc('day', block_timestamp) as day,
    tx_hash,
    origin_function_signature
  FROM
    core.core.ez_decoded_event_logs
  WHERE
    contract_address = lower('0xA4218e1F39DA4AaDaC971066458Db56e901bcbdE')
    and origin_function_signature = '0x252f7b01'
    and event_removed = 'FALSE'
    and tx_succeeded = 'TRUE'
),
total as(
  select
    fl.day as day,
    count(ft.tx_hash) as tx_count,
    sum(ft.tx_fee_precise) as total_tx_fee
  FROM
    filtered_logs fl
    JOIN core.core.fact_transactions ft on fl.tx_hash = ft.tx_hash
    AND fl.origin_function_signature = ft.origin_function_signature
  WHERE
    ft.tx_succeeded = 'TRUE'
  group BY
    fl.day
  order BY
    fl.day
),
total2 as(
  SELECT
    *,
    sum(total_tx_fee) over (
      order by
        day
    ) as cumulative_fee,
    sum(tx_count) over (
      order by
        day
    ) as cumulative_transaction
  FROM
    total
  ORDER by
    day
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
  t.day as day,
  t.tx_count,
  t.total_tx_fee,
  t.cumulative_fee,
  t.cumulative_transaction,
  (t.total_tx_fee * cp.price) as value_usd,
  sum(value_usd) over (
    order by
      day
  ) as cumulative_value_usd,
FROM
  total2 t
  JOIN core_price cp on t.day = cp.date
where
  day >= '2024-01-01'
order by
  t.day DESC
