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
latest_owner_amount as (
  select
    address,
    count(1) as NFT_OWNED
  from
    latest_owner
  group by
    all
  order by
    2 desc
),
team_mint_table as (
  SELECT
    *
  FROM
    core.nft.ez_nft_transfers
  where
    date_trunc ('day', block_timestamp) = '2024-12-12'
    and contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    and is_mint = 'TRUE'
),
team_NFT_owned as (
  SELECT
    to_address as address,
    sum(quantity) as number_of_NFT,
  FROM
    team_mint_table
  GROUP by
    1
  ORDER BY
    2 DESC
)
SELECT
  tno.address,
  tno.number_of_NFT as team_NFT_owned_at_mint,
  coalesce(loa.NFT_owned, 0) as team_current_NFT_owned,
  sum(coalesce(loa.nft_owned, 0)) over () cumulative_team_address_current_own,
  sum(number_of_NFT) over () cumulative_owned_at_mint,
FROM
  team_NFT_owned tno
  left join latest_owner_amount loa on tno.address = loa.address
