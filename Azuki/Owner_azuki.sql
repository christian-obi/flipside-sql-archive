-- --azuki => '0xed5af388653567af2f388e6224dc7c4b3241c544'  
-- --azuki elementals => '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'  
-- --azuki red beans => '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949'  
-- --azuki elemental beanz => '0x3af2a97414d1101e2107a70e7f33955da1346305'
WITH base AS (
  SELECT
    buyer_address,
    tokenid,
    ROW_NUMBER() OVER (
      PARTITION BY tokenid
      ORDER BY
        block_timestamp DESC
    ) AS rn
  FROM
    ethereum.nft.ez_nft_sales
  WHERE
    nft_address = '0xed5af388653567af2f388e6224dc7c4b3241c544'
),
holders AS (
  SELECT
    buyer_address AS holder,
    COUNT(tokenid) AS nft_count
  FROM
    base
  WHERE
    rn = 1 -- Only the most recent owners
  GROUP BY
    buyer_address
),
ownership_distribution AS (
  SELECT
    CASE
      WHEN nft_count = 1 THEN '1 NFT'
      WHEN nft_count = 2 THEN '2 NFTs'
      WHEN nft_count BETWEEN 3
      AND 5 THEN '3-5 NFTs'
      WHEN nft_count BETWEEN 6
      AND 10 THEN '6-10 NFTs'
      ELSE '11+ NFTs'
    END AS ownership_range,
    COUNT(holder) AS holder_count
  FROM
    holders
  GROUP BY
    ownership_range
),
total_holders AS (
  SELECT
    COUNT(DISTINCT holder) AS total
  FROM
    holders
)
SELECT
  ownership_distribution.ownership_range,
  ownership_distribution.holder_count,
  ROUND(
    (
      ownership_distribution.holder_count :: FLOAT / total_holders.total
    ) * 100,
    2
  ) AS percentage
FROM
  ownership_distribution,
  total_holders
ORDER BY
  percentage DESC;
