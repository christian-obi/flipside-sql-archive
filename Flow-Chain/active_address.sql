SELECT
  *
FROM
  flow.core.ez_token_transfers
where
  amount > 5
  and block_timestamp > '2024-08-01'
limit
  10
