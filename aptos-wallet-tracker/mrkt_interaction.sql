-- multiple market place interaction
SELECT
  buyer_address,
  count(DISTINCT platform_name) as marketplaces_count
FROM
  aptos.nft.ez_nft_sales
GROUP BY
  1
HAVING
  count(DISTINCT platform_name) > 1
ORDER BY
  marketplaces_count DESC
