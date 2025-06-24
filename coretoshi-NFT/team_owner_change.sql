WITH latest_owner AS (
  SELECT
    to_address AS address,
    token_id
  FROM
    core.nft.ez_nft_transfers
  WHERE
    block_timestamp >= '2024-12-12'
    AND contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf' QUALIFY ROW_NUMBER() OVER (
      PARTITION BY token_id
      ORDER BY
        block_timestamp DESC
    ) = 1
),
latest_owner_amount AS (
  SELECT
    address,
    COUNT(1) AS NFT_OWNED
  FROM
    latest_owner
  GROUP BY
    address
),
team_mint_table AS (
  SELECT
    *
  FROM
    core.nft.ez_nft_transfers
  WHERE
    DATE_TRUNC('day', block_timestamp) = '2024-12-12'
    AND contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    AND is_mint = 'TRUE'
),
team_NFT_owned AS (
  SELECT
    to_address AS address,
    SUM(quantity) AS number_of_NFT
  FROM
    team_mint_table
  GROUP BY
    to_address
)
SELECT
  'team_NFT_holding_at_mint' AS owned,
  SUM(tno.number_of_NFT) AS amount
FROM
  team_NFT_owned tno
  LEFT JOIN latest_owner_amount loa ON tno.address = loa.address
UNION
ALL
SELECT
  'team_NFT_current_holding' AS owned,
  SUM(COALESCE(loa.NFT_OWNED, 0)) AS amount
FROM
  team_NFT_owned tno
  LEFT JOIN latest_owner_amount loa ON tno.address = loa.address
