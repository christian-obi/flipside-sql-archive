--floor price by platform from september
SELECT
  platform_name,
  min(price) as floor_price,
  min(platform_fee) as min_platform_fee,
  currency_symbol
FROM
  ethereum.nft.ez_nft_sales
WHERE
  nft_address = lower('0x86486fE85545b57D06330acf1f3D63bb7b790cB4')
  and block_timestamp > '2024-09-01'
  and currency_symbol = 'ETH'
GROUP BY
  platform_name,
  currency_symbol
