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
og_mint_table as (
  SELECT
    *
  FROM
    core.nft.ez_nft_transfers
  where
    date_trunc ('day', block_timestamp) = '2024-12-13'
    and contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    and is_mint = 'TRUE'
),
og_NFT_owned as (
  SELECT
    to_address as address,
    sum(quantity) as number_of_NFT,
    count(address) over () as total_og_address,
    sum(number_of_NFT) over () as total_og_mint
  FROM
    og_mint_table
  GROUP by
    1
  ORDER BY
    2 DESC
)
SELECT
  'OG_NFT_holding_at_mint' as owned,
  sum(ono.number_of_NFT) as amount,
FROM
  og_NFT_owned ono
  left join latest_owner_amount loa on ono.address = loa.address
union
all
select
  'OG_NFT_current_holding' as owned,
  sum(coalesce(loa.NFT_owned, 0)) as amount
FROM
  og_NFT_owned ono
  left join latest_owner_amount loa on ono.address = loa.address
