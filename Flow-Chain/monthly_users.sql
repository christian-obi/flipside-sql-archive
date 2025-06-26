--this code derives the users and volume on flowchain grouping by month
SELECT
  count(DISTINCT sender) as user,
  /*month(block_timestamp) as month,*/
  date_trunc('month', block_timestamp) as month,
  --date_trunc('quarter',block_timestamp) as quarter,
  sum(amount) as flow_volume,
  count(EZ_TOKEN_TRANSFERS_ID) as no_trans
FROM
  flow.core.ez_token_transfers
WHERE
  tx_succeeded = TRUE
  AND block_timestamp >= '2024-01-01'
group BY
  2
order BY
  2
