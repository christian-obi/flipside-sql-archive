--this code derives users and volume on flowchain grouping by quarter
SELECT
  count(DISTINCT sender) as user,
  date_trunc('quarter', block_timestamp) as quarter,
  sum(amount) as flow_volume,
  CASE
    when quarter >= '2024-01-01'
    and quarter < '2024-04-01' then '1st quater'
    when quarter >= '2024-04-01'
    AND quarter < '2024-07-01' then '2nd quarter'
    when quarter >= '2024-07-01'
    AND quarter < '2024-10-01' then '3rd quarter'
    when quarter >= '2024-10-01'
    AND quarter < '2025-01-01' then '4th quarter'
    else NULL
  end as quater_name
FROM
  flow.core.ez_token_transfers
WHERE
  tx_succeeded = TRUE
  AND block_timestamp >= '2024-01-01'
group BY
  2
order BY
  2
