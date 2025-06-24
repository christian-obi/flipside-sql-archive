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
)
select
  address,
  count(1) as NFT_OWNED
from
  latest_owner
group by
  all
order by
  2 desc
