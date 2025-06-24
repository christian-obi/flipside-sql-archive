WITH mint_table AS (
  SELECT
    *
  FROM
    core.nft.ez_nft_transfers
  WHERE
    DATE_TRUNC('day', block_timestamp) >= '2024-12-12'
    AND DATE_TRUNC('day', block_timestamp) <= '2024-12-14'
    AND contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    AND is_mint = 'TRUE'
),
minting AS (
  SELECT
    to_address AS address,
    SUM(quantity) AS number_of_NFT_mint
  FROM
    mint_table
  GROUP BY
    to_address
),
latest_owner AS (
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
)
SELECT
  lo.address,
  COUNT(1) AS NFT_OWNED
FROM
  latest_owner lo
  LEFT JOIN minting m ON lo.address = m.address
WHERE
  m.address IS NULL -- Exclude addresses that minted NFTs
  AND number_of_NFT_mint is NULL
GROUP BY
  lo.address
ORDER BY
  NFT_OWNED DESC
