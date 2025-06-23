WITH
-- Get unique NFT owners
nft_owners AS (
  SELECT 
 distinct purple_fren_buyers AS owner_address 
FROM $query('ae50929c-9227-422d-8eda-7a7e04d296e1')
where purple_fren_buyers is not null
),

-- Get unique swappers
swappers AS (
  SELECT DISTINCT user_address 
FROM $query('6314a1b1-7ca1-445f-9f9b-ec92a14c351d')
),

-- People who both own NFTs and have done swaps
nft_swappers AS (
  SELECT n.owner_address
  FROM nft_owners n
  INNER JOIN swappers s ON n.owner_address = s.user_address
where owner_address is not null

)

-- Final aggregation
SELECT
  (SELECT COUNT(*) FROM nft_owners) AS total_nft_owners,
  (SELECT COUNT(*) FROM nft_swappers) AS nft_owners_that_swapped,
  ROUND(
    (SELECT COUNT(*) FROM nft_swappers) * 100.0 / NULLIF((SELECT COUNT(*) FROM nft_owners), 0),
    2
  ) AS percent_nft_owners_that_swapped

