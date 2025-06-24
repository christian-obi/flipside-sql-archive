with tab1 as (
  select
    to_address as user,
    count(distinct token_id) as "Number of NFTs"
  from
    core.nft.ez_nft_transfers
  where
    contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    and name = 'Coretoshi'
    and is_mint = 'TRUE'
    and origin_function_signature = '0xaa4daf24'
    and from_address = '0x0000000000000000000000000000000000000000'
    and token_transfer_type = 'erc721_Transfer'
  group by
    1
)
select
  "Number of NFTs",
  count(distinct user) as "Number of Users"
from
  tab1
group by
  1
order by
  1
