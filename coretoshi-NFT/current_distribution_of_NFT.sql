with latest_owner as (
  select
    to_address as address,
    token_id
  FROM
    core.nft.ez_nft_transfers
  where
    block_timestamp >= '2024-12-12'
    and contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf' qualify row_number() over(
      partition by token_id
      order by
        block_timestamp DESC
    ) = 1
),
tokens as (
  SELECT
    count (DISTINCT token_id) as "Number of NTFS",
    address as user
  FROM
    latest_owner
  GROUP by
    2
)
SELECT
  "Number of NTFS",
  count(DISTINCT user) as "number of users"
FROM
  tokens
group by
  1
order by
  1
