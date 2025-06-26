-- volume and price
SELECT
  date_trunc('month', block_timestamp) as sale_date,
  platform_name,
  sum(price) as total_volume,
  avg(price) as avg_price
from
  ethereum.nft.ez_nft_sales
WHERE
  nft_address = lower('0x86486fE85545b57D06330acf1f3D63bb7b790cB4')
GROUP BY
  sale_date,
  platform_name
ORDER by
  sale_date
