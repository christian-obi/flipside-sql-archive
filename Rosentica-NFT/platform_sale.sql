with base as (
  SELECT
    --block_timestamp::date as date ,
    date_trunc('month', block_timestamp) as date,
    platform_name,
    count(*) as sales_count,
  FROM
    ethereum.nft.ez_nft_sales
  WHERE
    nft_address = lower('0x86486fE85545b57D06330acf1f3D63bb7b790cB4')
  GROUP BY
    1,
    2 --ORDER BY 3
)
SELECT
  date,
  platform_name,
  sum(sales_count) as platform_volume
from
  base
GROUP BY
  all
ORDER BY
  1
