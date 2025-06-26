-- number of sales
SELECT
  date_trunc('week', block_timestamp) as date,
  count(tx_hash) as no_sales,
  platform_name
FROM
  aptos.nft.ez_nft_sales
GROUP by
  1,
  3
ORDER BY
  1
