with from_address_table as (
  select
    from_address as address
  FROM
    core.nft.ez_nft_transfers
  WHERE
    name = 'Coretoshi'
    and token_standard = 'erc721'
    and is_mint = 'FALSE'
),
to_address_table as (
  select
    to_address as address
  FROM
    core.nft.ez_nft_transfers
  WHERE
    name = 'Coretoshi'
    and token_standard = 'erc721'
),
addresses as (
  SELECT
    address
  FROM
    from_address_table
  UNION
  ALL
  SELECT
    address
  FROM
    to_address_table
)
SELECT
  count(DISTINCT address) as number_of_addresses
from
  addresses
