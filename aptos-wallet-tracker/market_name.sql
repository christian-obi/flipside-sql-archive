-- multiple market by name
SELECT
  buyer_address,
  listagg(DISTINCT platform_name, ',') as marketplaces
FROM
  aptos.nft.ez_nft_sales
group by
  1
having
  count(DISTINCT platform_name) > 1
ORDER BY
  1
