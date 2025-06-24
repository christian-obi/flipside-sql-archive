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
latest_owner_amount as(
  select
    address,
    count(1) as NFT_OWNED
  from
    latest_owner
  group by
    address
),
public_mint_table as (
  SELECT
    *
  FROM
    core.nft.ez_nft_transfers
  where
    date_trunc ('day', block_timestamp) = '2024-12-14'
    and contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    and is_mint = 'TRUE'
),
public_NFT_owned as (
  SELECT
    to_address as address,
    sum(quantity) as number_of_NFT,
  FROM
    public_mint_table
  GROUP by
    1
  ORDER BY
    2 DESC
)
SELECT
  'public_NFT_holding_at_mint' AS owned,
  SUM(pno.number_of_NFT) AS amount
FROM
  public_NFT_owned pno
  LEFT JOIN latest_owner_amount loa ON pno.address = loa.address
UNION
ALL
SELECT
  'public_NFT_current_holding' AS owned,
  SUM(COALESCE(loa.NFT_OWNED, 0)) AS amount
FROM
  public_NFT_owned pno
  LEFT JOIN latest_owner_amount loa ON pno.address = loa.address
