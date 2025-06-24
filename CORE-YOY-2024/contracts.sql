with first_use as (
  SELECT
    contract_address,
    min(block_timestamp) as first_used_date
  FROM
    core.core.ez_decoded_event_logs
  group BY
    1
)
SELECT
  date_trunc('week', first_used_date) as date,
  count(DISTINCT contract_address) as n_contracts,
  sum(count(DISTINCT contract_address)) over (
    ORDER by
      date
  ) as cum_contracts
FROM
  first_use
WHERE
  date >= '2024-01-01'
GROUP BY
  1
ORDER BY
  1 DESC
